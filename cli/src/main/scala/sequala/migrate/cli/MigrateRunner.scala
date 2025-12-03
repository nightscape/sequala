package sequala.migrate.cli

import sequala.migrate.*
import sequala.common.{AnsiDialect, Dialect, Dialects, FilePatternResolver, OracleDialect, PostgresDialect}
import sequala.parse.{
  DirectoryDestination,
  FileDestination,
  JqHelper,
  JqHelperStatic,
  OutputDestination,
  StatementTransform,
  TransformFactory,
  TransformFactoryTokenStore,
  TransformResult,
  TransformWithWrites,
  Transforms
}
import sequala.schema.{CreateIndex, CreateTable, SchemaDiffOp, Statement}
import sequala.schema.ast.Name
import sequala.schema.oracle.{OracleColumnComment, OracleTableComment}
import sequala.DbConfig

import java.io.{File, PrintWriter}
import java.nio.file.Path
import java.sql.{Connection, DriverManager}
import scala.io.StdIn
import scala.util.{Failure, Success, Try, Using}
import scala.concurrent.duration.*
import io.circe.{Encoder, Json, Printer}
import io.circe.syntax.*

object MigrateRunner:

  private def resolveSchemas(connection: Connection, schemaPattern: String, dialect: Dialect): Seq[String] =
    if schemaPattern.contains("%") || schemaPattern.contains("_") then
      dialect match
        case OracleDialect =>
          import sequala.migrate.oracle.OracleSchemaInspector
          OracleSchemaInspector.getSchemaNames(connection, Some(schemaPattern))
        case _ => Seq(schemaPattern)
    else Seq(schemaPattern)

  def runPlan(
    source: String,
    db: DbConfig,
    format: String,
    pretty: Boolean,
    writeTo: OutputDestination,
    transform: Option[TransformFactory],
    sourceTransform: Option[TransformFactory]
  ): Unit =
    val transformTokens = transform.flatMap(TransformFactoryTokenStore.get)
    val sourceTransformTokens = sourceTransform.flatMap(TransformFactoryTokenStore.get)

    val configResult = for
      config <- ConfigLoader.loadConfig(db.config.map(_.toString), db.env)
      dbConfig <- ConfigLoader.loadDatabaseConfig(
        config,
        urlOverride = db.database,
        userOverride = db.user,
        passwordOverride = db.password,
        schemaOverride = db.schema,
        dialectOverride = db.dialect
      )
    yield dbConfig

    configResult match
      case Left(error) =>
        System.err.println(s"Error: $error")
        System.exit(1)
      case Right(dbConfig) =>
        val dialect = dbConfig.inferredDialect
        runPlanWithDialect(
          source,
          dbConfig,
          dialect,
          format,
          pretty,
          writeTo,
          transform,
          sourceTransform,
          transformTokens,
          sourceTransformTokens
        )

  private def runPlanWithDialect(
    source: String,
    dbConfig: DatabaseConfig,
    dialect: Dialect,
    format: String,
    pretty: Boolean,
    writeTo: OutputDestination,
    transform: Option[TransformFactory],
    sourceTransform: Option[TransformFactory],
    transformTokens: Option[Seq[String]] = None,
    sourceTransformTokens: Option[Seq[String]] = None
  ): Unit =
    val files = FilePatternResolver.resolve(source)
    if files.isEmpty then
      System.err.println(s"Error: No files match pattern: $source")
      System.exit(1)

    val parsedTables: Seq[dialect.DialectTable] = parseMultipleFiles(files, dialect) match
      case Right(tables) => tables
      case Left(err) =>
        System.err.println(s"Error: $err")
        System.exit(1)
        return

    val sourceFilePaths = files.map(_.getAbsolutePath)

    val (allTables, sourceWrites): (Seq[dialect.DialectTable], Map[String, String]) = sourceTransform match
      case Some(factory) =>
        sourceTransformTokens match
          case Some(tokens) =>
            Transforms.resolveTransformsWithWrites(tokens, dialect, sourceFilePaths, pretty) match
              case Right((transforms, writeCollector)) =>
                applySourceTransform(parsedTables, transforms, dialect) match
                  case Right(tables) =>
                    val statements =
                      parsedTables.map(t => CreateTable(t.asInstanceOf[dialect.DialectTable]).asInstanceOf[Statement])
                    val writes = writeCollector(statements)
                    (tables, writes)
                  case Left(err) =>
                    System.err.println(s"Error applying source transform: $err")
                    System.exit(1)
                    return
              case Left(err) =>
                System.err.println(s"Error resolving source transforms: $err")
                System.exit(1)
                return
          case None =>
            factory(dialect, sourceFilePaths).flatMap { transforms =>
              applySourceTransform(parsedTables, transforms, dialect)
            } match
              case Right(tables) => (tables, Map.empty[String, String])
              case Left(err) =>
                System.err.println(s"Error applying source transform: $err")
                System.exit(1)
                return
      case None => (parsedTables, Map.empty[String, String])

    sourceWrites.foreach { case (path, content) =>
      writeTo.write(content, Some(path))
      println(s"  Wrote: $path")
    }

    val connection = createConnection(dbConfig) match
      case Right(conn) => conn
      case Left(err) =>
        System.err.println(s"Error: $err")
        System.exit(1)
        return

    try
      val tablesBySchema: Map[Option[String], Seq[dialect.DialectTable]] = allTables.groupBy(_.schema)

      val schemasToProcess = tablesBySchema.keys.flatten.toSeq.distinct match
        case Seq() => resolveSchemas(connection, dbConfig.schema, dialect)
        case schemas => schemas

      val (outputTransforms, outputWriteCollector): (Seq[StatementTransform], Seq[Statement] => Map[String, String]) =
        transform match
          case Some(factory) =>
            transformTokens match
              case Some(tokens) =>
                Transforms.resolveTransformsWithWrites(tokens, dialect, Seq.empty, pretty) match
                  case Right((transforms, writeCollector)) => (transforms, writeCollector)
                  case Left(err) =>
                    System.err.println(s"Error resolving transforms: $err")
                    sys.exit(1)
              case None =>
                factory(dialect, Seq.empty) match
                  case Right(transforms) => (transforms, (_: Seq[Statement]) => Map.empty[String, String])
                  case Left(err) =>
                    System.err.println(s"Error resolving transforms: $err")
                    sys.exit(1)
          case None => (Seq.empty, (_: Seq[Statement]) => Map.empty[String, String])

      val allDiffs = schemasToProcess.map { schemaName =>
        val desiredTables: Seq[dialect.DialectTable] =
          tablesBySchema.getOrElse(Some(schemaName), Seq.empty) ++
            tablesBySchema.getOrElse(None, Seq.empty)
        val currentTables = dialect.inspectSchema(connection, schemaName)
        val diffs = dialect.diffSchemas(currentTables, desiredTables, schemaName)

        val filteredDiffs =
          if outputTransforms.nonEmpty then
            val statements = diffs.collect { case s: Statement => s }
            val transformed = Transforms.apply(statements, outputTransforms)
            transformed.collect { case op: SchemaDiffOp => op }
          else diffs

        (schemaName, filteredDiffs)
      }

      val allStatements = allDiffs.flatMap(_._2).collect { case s: Statement => s }
      val outputWrites = outputWriteCollector(allStatements)
      outputWrites.foreach { case (path, content) =>
        writeTo.write(content, Some(path))
        println(s"  Wrote: $path")
      }

      writeTo match
        case DirectoryDestination(baseDir) =>
          java.nio.file.Files.createDirectories(java.nio.file.Paths.get(baseDir))
          allDiffs.foreach { case (schema, diffs) =>
            if diffs.nonEmpty then
              val content = dialect.formatMigrationPlan(diffs, format)
              val outputPath = java.nio.file.Paths.get(baseDir, s"$schema-migration.sql")
              java.nio.file.Files.writeString(outputPath, content)
              println(s"  $schema: ${diffs.size} statements → $outputPath")
          }
          if allDiffs.size > 1 then
            println(s"\nTotal: ${allDiffs.map(_._2.size).sum} statements across ${allDiffs.size} schemas")
        case _ =>
          val combinedDiffs = allDiffs.flatMap(_._2)
          if combinedDiffs.isEmpty then writeTo.write("")
          else writeTo.write(dialect.formatMigrationPlan(combinedDiffs, format))
    finally connection.close()

  private def parseMultipleFiles(files: Seq[File], dialect: Dialect): Either[String, Seq[dialect.DialectTable]] =
    val tables = files.flatMap { file =>
      dialect.parseFile(file.getAbsolutePath) match
        case Right(tables) => tables
        case Left(err) =>
          System.err.println(s"Warning: Failed to parse ${file.getAbsolutePath}: $err")
          Seq.empty[dialect.DialectTable]
    }

    if tables.isEmpty then Left("No valid tables found in source files")
    else Right(tables)

  private def applySourceTransform(
    tables: Seq[?],
    transforms: Seq[StatementTransform],
    dialect: Dialect
  ): Either[String, Seq[dialect.DialectTable]] =
    val statements: Seq[Statement] = tables.map { t =>
      CreateTable(t.asInstanceOf[dialect.DialectTable]).asInstanceOf[Statement]
    }

    val transformed = Transforms.apply(statements, transforms)
    Right(dialect.tablesFromStatements(transformed))

  def runApply(
    source: String,
    db: DbConfig,
    dryRun: Boolean,
    autoApprove: Boolean,
    transactional: Boolean,
    pretty: Boolean
  ): Unit =
    val result = for
      config <- ConfigLoader.loadConfig(db.config.map(_.toString), db.env)
      dbConfig <- ConfigLoader.loadDatabaseConfig(
        config,
        urlOverride = db.database,
        userOverride = db.user,
        passwordOverride = db.password,
        schemaOverride = db.schema,
        dialectOverride = db.dialect
      )
      dialect = dbConfig.inferredDialect
      desiredTables <- dialect.parseFile(source)
      connection <- createConnection(dbConfig)
      currentTables = dialect.inspectSchema(connection, dbConfig.schema)
      diffs = dialect.diffSchemas(currentTables, desiredTables, dbConfig.schema)
    yield (diffs, dialect, connection)

    result match
      case Left(error) =>
        System.err.println(s"Error: $error")
        System.exit(1)
      case Right((diffs, dialect, connection)) =>
        try
          if diffs.isEmpty then println("No changes detected. Schema is up to date.")
          else if dryRun then
            println("=== DRY RUN MODE ===")
            println(dialect.formatMigrationPlan(diffs, "sql"))
            println("\nNo changes were applied (dry run mode).")
          else
            println(dialect.formatMigrationPlan(diffs, "text"))
            println()

            val proceed = autoApprove || promptConfirmation()
            if proceed then
              println("\nApplying migration...")
              val mode = if transactional then TransactionMode.Transactional else TransactionMode.AutoCommit
              val execResult = dialect.executeMigration(connection, diffs, mode)
              if execResult.successful then
                println(s"\n✓ Migration completed successfully!")
                println(
                  s"  Executed ${execResult.executedCount} steps in ${formatDuration(execResult.totalExecutionTime)}"
                )
              else
                System.err.println(s"\n✗ Migration failed!")
                execResult.failedStep.foreach { step =>
                  System.err.println(
                    s"  Failed at step ${step.stepIndex}: ${step.error.map(_.getMessage).getOrElse("Unknown error")}"
                  )
                  System.err.println(s"  SQL: ${step.sql}")
                }
                System.exit(1)
            else println("Migration cancelled.")
        finally connection.close()

  def runInspect(db: DbConfig, output: Option[Path], tableFilter: Option[String], pretty: Boolean): Unit =
    val result = for
      config <- ConfigLoader.loadConfig(db.config.map(_.toString), db.env)
      dbConfig <- ConfigLoader.loadDatabaseConfig(
        config,
        urlOverride = db.database,
        userOverride = db.user,
        passwordOverride = db.password,
        schemaOverride = db.schema,
        dialectOverride = db.dialect
      )
      dialect = dbConfig.inferredDialect
      connection <- createConnection(dbConfig)
    yield (dbConfig, dialect, connection)

    result match
      case Left(error) =>
        System.err.println(s"Error: $error")
        System.exit(1)
      case Right((dbConfig, dialect, connection)) =>
        try
          val schemasToProcess = resolveSchemas(connection, dbConfig.schema, dialect)

          val allTables = schemasToProcess.flatMap { schemaName =>
            dialect.inspectSchema(connection, schemaName)
          }

          val filteredTables = tableFilter match
            case Some(pattern) =>
              val regex = globToRegex(pattern)
              allTables.filter { t =>
                regex.matches(t.name) || regex.matches(t.name.toLowerCase)
              }
            case None => allTables

          val ddl = dialect.renderTables(filteredTables.asInstanceOf[Seq[dialect.DialectTable]], pretty)

          output match
            case Some(outputPath) =>
              Using(new PrintWriter(outputPath.toFile)) { writer =>
                writer.println(ddl)
              } match
                case Success(_) => println(s"Schema written to $outputPath")
                case Failure(e) =>
                  System.err.println(s"Failed to write output file: ${e.getMessage}")
                  System.exit(1)
            case None =>
              println(ddl)
        finally connection.close()

  def runDiff(
    from: String,
    to: String,
    dialect: Dialect,
    format: String,
    pretty: Boolean,
    writeTo: OutputDestination,
    transform: Option[TransformFactory]
  ): Unit =
    val result = for
      fromTables <- dialect.parseFile(from)
      toTables <- dialect.parseFile(to)
      diffs = dialect.diffSchemas(fromTables, toTables, "")
    yield diffs

    result match
      case Left(error) =>
        System.err.println(s"Error: $error")
        System.exit(1)
      case Right(diffs) =>
        val filteredDiffs = transform match
          case Some(factory) =>
            factory(dialect, Seq.empty) match
              case Right(transforms) =>
                val statements = diffs.collect { case s: Statement => s }
                val transformed = Transforms.apply(statements, transforms)
                transformed.collect { case op: SchemaDiffOp => op }
              case Left(err) =>
                System.err.println(s"Error resolving transforms: $err")
                sys.exit(1)
          case None => diffs

        if filteredDiffs.isEmpty then writeTo.write("")
        else writeTo.write(dialect.formatMigrationPlan(filteredDiffs, format))

  private def createConnection(dbConfig: DatabaseConfig): Either[String, Connection] =
    dbConfig.inferredDialect match
      case OracleDialect =>
        Try(Class.forName("oracle.jdbc.OracleDriver"))
      case _ => ()

    Try {
      val props = new java.util.Properties()
      dbConfig.user.foreach(props.setProperty("user", _))
      dbConfig.password.foreach(props.setProperty("password", _))
      DriverManager.getConnection(dbConfig.jdbcUrl, props)
    } match
      case Success(conn) => Right(conn)
      case Failure(e) => Left(s"Failed to connect to database: ${e.getMessage}")

  private def promptConfirmation(): Boolean =
    print("Do you want to apply these changes? [y/N] ")
    Console.flush()
    val input = StdIn.readLine()
    input != null && input.trim.toLowerCase == "y"

  private def formatDuration(d: Duration): String =
    if d.toMillis < 1000 then s"${d.toMillis}ms"
    else if d.toSeconds < 60 then f"${d.toMillis / 1000.0}%.2fs"
    else s"${d.toMinutes}m ${d.toSeconds % 60}s"

  def runDump(
    db: DbConfig,
    ddls: Option[String],
    data: Option[String],
    outputDir: Path,
    batchSize: Int,
    pretty: Boolean,
    ddlsFilter: Option[Path],
    dataFilter: Option[Path],
    filter: Option[Path]
  ): Unit =
    if filter.isDefined && (ddlsFilter.isDefined || dataFilter.isDefined) then
      System.err.println(
        "Error: --filter cannot be used together with --ddls-filter or --data-filter. Use --filter for combined processing or --ddls-filter/--data-filter for separate processing."
      )
      System.exit(1)

    val result = for
      config <- ConfigLoader.loadConfig(db.config.map(_.toString), db.env)
      dbConfig <- ConfigLoader.loadDatabaseConfig(
        config,
        urlOverride = db.database,
        userOverride = db.user,
        passwordOverride = db.password,
        schemaOverride = db.schema,
        dialectOverride = db.dialect
      )
      dialect = dbConfig.inferredDialect
      connection <- createConnection(dbConfig)
    yield (dbConfig, dialect, connection)

    result match
      case Left(error) =>
        System.err.println(s"Error: $error")
        System.exit(1)
      case Right((dbConfig, dialect, connection)) =>
        try
          java.nio.file.Files.createDirectories(outputDir)

          val jqHelper = JqHelper(dialect)
          given Encoder[Statement] = dialect.codecs.statementEncoder

          val tablesBySchema = scala.collection.mutable.Map[String, Seq[dialect.DialectTable]]()
          val dataBySchema = scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, Json]]()

          ddls.foreach { ddlPattern =>
            if ddlPattern.contains(".") then
              parseDataPattern(ddlPattern) match
                case Left(err) =>
                  System.err.println(s"Error: $err")
                  System.exit(1)
                case Right((ddlSchema, tablePattern)) =>
                  val tableNames = dialect.name match
                    case "oracle" =>
                      import sequala.migrate.oracle.OracleSchemaInspector
                      OracleSchemaInspector.getTableNames(connection, ddlSchema, Some(tablePattern))
                    case _ =>
                      dialect.inspectSchema(connection, ddlSchema).map(_.name).filter { name =>
                        val regex = sqlLikeToRegex(tablePattern)
                        regex.matches(name) || regex.matches(name.toLowerCase)
                      }

                  if tableNames.nonEmpty then
                    val allTables = dialect.inspectSchema(connection, ddlSchema)
                    val tables = tableNames.flatMap { name =>
                      allTables.find(_.name.equalsIgnoreCase(name))
                    }
                    val existing = tablesBySchema.getOrElse(ddlSchema, Seq.empty)
                    tablesBySchema(ddlSchema) = existing ++ tables
            else
              val schemas = dialect.name match
                case "oracle" =>
                  import sequala.migrate.oracle.OracleSchemaInspector
                  OracleSchemaInspector.getSchemaNames(connection, Some(ddlPattern))
                case _ =>
                  Seq(dbConfig.schema)

              schemas.foreach { schema =>
                val tables = dialect.inspectSchema(connection, schema)
                if tables.nonEmpty then
                  val existing = tablesBySchema.getOrElse(schema, Seq.empty)
                  tablesBySchema(schema) = existing ++ tables
              }
          }

          data.foreach { dataPattern =>
            parseDataPattern(dataPattern) match
              case Left(err) =>
                System.err.println(s"Error: $err")
                System.exit(1)
              case Right((dataSchema, tablePattern)) =>
                val tableNames = dialect.name match
                  case "oracle" =>
                    import sequala.migrate.oracle.OracleSchemaInspector
                    OracleSchemaInspector.getTableNames(connection, dataSchema, Some(tablePattern))
                  case _ =>
                    dialect.inspectSchema(connection, dataSchema).map(_.name).filter { name =>
                      val regex = sqlLikeToRegex(tablePattern)
                      regex.matches(name) || regex.matches(name.toLowerCase)
                    }

                val schemaData = dataBySchema.getOrElseUpdate(dataSchema, scala.collection.mutable.Map.empty)
                tableNames.foreach { tableName =>
                  val tableJson = DataExporter.exportTableDataAsJson(connection, dataSchema, tableName, dialect.name)
                  schemaData(tableName) = tableJson
                }
          }

          filter match
            case Some(filterPath) =>
              val query = JqHelperStatic.readQueryFile(filterPath.toString)
              val writeDirectives = JqHelperStatic.parseWriteDirectives(query)

              val allTablesJson = tablesBySchema.toSeq.flatMap { case (schema, tables) =>
                tables.flatMap { table =>
                  import sequala.schema.oracle.OracleIndexOptions

                  val createTable = CreateTable(table)
                  val createTableJson = Json.obj(
                    "schema" -> Json.fromString(schema),
                    "table" -> Json.fromString(table.name),
                    "statement" -> createTable.asInstanceOf[Statement].asJson
                  )

                  // Generate CreateIndex statements for non-constraint indexes
                  val indexJsons = table.indexes.flatMap { idx =>
                    idx.name.map { idxName =>
                      val qualifiedTableName = s""""$schema"."${table.name}""""
                      val qualifiedIndexName = s""""$schema"."$idxName""""
                      val createIndex = CreateIndex[OracleIndexOptions](
                        name = qualifiedIndexName,
                        tableName = qualifiedTableName,
                        columns = idx.columns,
                        unique = idx.unique,
                        ifNotExists = false,
                        options = OracleIndexOptions(where = idx.where)
                      )
                      Json.obj(
                        "schema" -> Json.fromString(schema),
                        "table" -> Json.fromString(table.name),
                        "statement" -> createIndex.asInstanceOf[Statement].asJson
                      )
                    }
                  }

                  val commentJsons = if dialect.name == "oracle" then
                    val qualifiedTableName = s""""$schema"."${table.name}""""
                    val tableCommentJsons = table.comment.toSeq.map { c =>
                      val commentStmt = OracleTableComment(Name(qualifiedTableName), c)
                      Json.obj(
                        "schema" -> Json.fromString(schema),
                        "table" -> Json.fromString(table.name),
                        "statement" -> commentStmt.asInstanceOf[Statement].asJson
                      )
                    }
                    val columnCommentJsons = table.columns.flatMap { col =>
                      col.comment.map { c =>
                        val commentStmt = OracleColumnComment(Name(s"""$qualifiedTableName."${col.name}""""), c)
                        Json.obj(
                          "schema" -> Json.fromString(schema),
                          "table" -> Json.fromString(table.name),
                          "statement" -> commentStmt.asInstanceOf[Statement].asJson
                        )
                      }
                    }
                    tableCommentJsons ++ columnCommentJsons
                  else Seq.empty

                  createTableJson +: (indexJsons ++ commentJsons)
                }
              }

              val allDataJson = dataBySchema.toSeq.flatMap { case (schema, tables) =>
                tables.map { case (tableName, tableJson) =>
                  Json.obj(
                    "schema" -> Json.fromString(schema),
                    "table" -> Json.fromString(tableName),
                    "data" -> tableJson
                  )
                }
              }

              // Extract setup info (tablespaces and schemas with quotas) for Oracle
              val setupJson = if dialect.name == "oracle" then
                import sequala.schema.oracle.{OracleTableOptions, CreateTablespace, CreateUser, UserQuota}

                // Extract unique tablespaces from all tables
                val tablespaces = tablesBySchema.values.flatten
                  .flatMap { table =>
                    table.options match
                      case opts: OracleTableOptions => opts.tablespace
                      case _ => None
                  }
                  .toSet
                  .toSeq
                  .sorted

                // Build schema -> tablespaces mapping
                val schemaTablespaces = tablesBySchema.map { case (schema, tables) =>
                  val tsForSchema = tables
                    .flatMap { table =>
                      table.options match
                        case opts: OracleTableOptions => opts.tablespace
                        case _ => None
                    }
                    .toSet
                    .toSeq
                    .sorted
                  schema -> tsForSchema
                }

                // Generate CreateTablespace statements
                val tablespaceStmts = tablespaces.map { ts =>
                  CreateTablespace(ts).asInstanceOf[Statement].asJson
                }

                // Generate CreateUser statements with quotas
                val userStmts = schemaTablespaces.map { case (schema, tsForSchema) =>
                  val quotas = (tsForSchema ++ Seq("USERS")).distinct.map(ts => UserQuota(ts))
                  CreateUser(schema, "changeme", quotas = quotas).asInstanceOf[Statement].asJson
                }.toSeq

                Json.obj(
                  "tablespaces" -> tablespaceStmts.asJson,
                  "users" -> userStmts.asJson,
                  "tablespaceNames" -> tablespaces.asJson,
                  "schemaNames" -> tablesBySchema.keys.toSeq.sorted.asJson
                )
              else Json.obj()

              val combinedJson = Json.obj(
                "setup" -> setupJson,
                "ddl" -> Json.obj("tables" -> allTablesJson.asJson, "schemas" -> tablesBySchema.keys.toSeq.asJson),
                "data" -> Json.obj("tables" -> allDataJson.asJson, "schemas" -> dataBySchema.keys.toSeq.asJson)
              )

              val jqOutput = JqHelperStatic.executeJq(
                Printer.spaces2.copy(dropNullValues = true).print(combinedJson),
                query,
                s"from filter '$filterPath'"
              )

              if writeDirectives.nonEmpty then
                val writes = jqHelper.collectWrites(jqOutput, writeDirectives, "", pretty)
                writes.foreach { case (path, content) =>
                  val fullPath = outputDir.resolve(path)
                  java.nio.file.Files.createDirectories(fullPath.getParent)
                  java.nio.file.Files.writeString(fullPath, content)
                  println(s"Wrote: $fullPath")
                }
              else
                val outputFile = outputDir.resolve("dump.json")
                java.nio.file.Files.writeString(outputFile, jqOutput)
                println(s"Wrote: $outputFile")

            case None =>
              ddlsFilter match
                case Some(filterPath) =>
                  val query = JqHelperStatic.readQueryFile(filterPath.toString)
                  val writeDirectives = JqHelperStatic.parseWriteDirectives(query)

                  val allTablesJson = tablesBySchema.toSeq.flatMap { case (schema, tables) =>
                    tables.flatMap { table =>
                      import sequala.schema.oracle.OracleIndexOptions

                      val createTable = CreateTable(table)
                      val createTableJson = Json.obj(
                        "schema" -> Json.fromString(schema),
                        "statement" -> createTable.asInstanceOf[Statement].asJson
                      )

                      // Generate CreateIndex statements for non-constraint indexes
                      val indexJsons = table.indexes.flatMap { idx =>
                        idx.name.map { idxName =>
                          val qualifiedTableName = s""""$schema"."${table.name}""""
                          val qualifiedIndexName = s""""$schema"."$idxName""""
                          val createIndex = CreateIndex[OracleIndexOptions](
                            name = qualifiedIndexName,
                            tableName = qualifiedTableName,
                            columns = idx.columns,
                            unique = idx.unique,
                            ifNotExists = false,
                            options = OracleIndexOptions(where = idx.where)
                          )
                          Json.obj(
                            "schema" -> Json.fromString(schema),
                            "statement" -> createIndex.asInstanceOf[Statement].asJson
                          )
                        }
                      }

                      val commentJsons = if dialect.name == "oracle" then
                        val qualifiedTableName = s""""$schema"."${table.name}""""
                        val tableCommentJsons = table.comment.toSeq.map { c =>
                          val commentStmt = OracleTableComment(Name(qualifiedTableName), c)
                          Json.obj(
                            "schema" -> Json.fromString(schema),
                            "statement" -> commentStmt.asInstanceOf[Statement].asJson
                          )
                        }
                        val columnCommentJsons = table.columns.flatMap { col =>
                          col.comment.map { c =>
                            val commentStmt = OracleColumnComment(Name(s"""$qualifiedTableName."${col.name}""""), c)
                            Json.obj(
                              "schema" -> Json.fromString(schema),
                              "statement" -> commentStmt.asInstanceOf[Statement].asJson
                            )
                          }
                        }
                        tableCommentJsons ++ columnCommentJsons
                      else Seq.empty

                      createTableJson +: (indexJsons ++ commentJsons)
                    }
                  }

                  val inputJson =
                    Json.obj("tables" -> allTablesJson.asJson, "schemas" -> tablesBySchema.keys.toSeq.asJson)

                  val jqOutput = JqHelperStatic.executeJq(
                    Printer.spaces2.copy(dropNullValues = true).print(inputJson),
                    query,
                    s"from filter '$filterPath'"
                  )

                  if writeDirectives.nonEmpty then
                    val writes = jqHelper.collectWrites(jqOutput, writeDirectives, "", pretty)
                    writes.foreach { case (path, content) =>
                      val fullPath = outputDir.resolve(path)
                      java.nio.file.Files.createDirectories(fullPath.getParent)
                      java.nio.file.Files.writeString(fullPath, content)
                      println(s"Wrote: $fullPath")
                    }
                  else
                    val sql = jqHelper.jsonToSql(jqOutput, pretty)
                    val outputFile = outputDir.resolve("dump.sql")
                    java.nio.file.Files.writeString(outputFile, sql)
                    println(s"Wrote: $outputFile")

                case None =>
                  tablesBySchema.foreach { case (schema, tables) =>
                    if tables.nonEmpty then
                      val output = new StringBuilder()
                      output.append(s"-- DDL for schema '$schema'\n\n")
                      output.append(dialect.renderTables(tables.asInstanceOf[Seq[dialect.DialectTable]], pretty))
                      output.append("\n\n")

                      val outputFile = outputDir.resolve(s"$schema-dump.sql")
                      java.nio.file.Files.writeString(outputFile, output.toString)
                      println(s"Wrote: $outputFile")
                  }

          dataFilter match
            case Some(filterPath) =>
              val query = JqHelperStatic.readQueryFile(filterPath.toString)
              val writeDirectives = JqHelperStatic.parseWriteDirectives(query)

              val allDataJson = dataBySchema.toSeq.flatMap { case (schema, tables) =>
                tables.map { case (tableName, tableJson) =>
                  Json.obj(
                    "schema" -> Json.fromString(schema),
                    "table" -> Json.fromString(tableName),
                    "data" -> tableJson
                  )
                }
              }

              val inputJson = Json.obj("tables" -> allDataJson.asJson, "schemas" -> dataBySchema.keys.toSeq.asJson)

              val jqOutput = JqHelperStatic.executeJq(
                Printer.spaces2.copy(dropNullValues = true).print(inputJson),
                query,
                s"from filter '$filterPath'"
              )

              if writeDirectives.nonEmpty then
                val writes = jqHelper.collectWrites(jqOutput, writeDirectives, "", pretty)
                writes.foreach { case (path, content) =>
                  val fullPath = outputDir.resolve(path)
                  java.nio.file.Files.createDirectories(fullPath.getParent)
                  java.nio.file.Files.writeString(fullPath, content)
                  println(s"Wrote: $fullPath")
                }
              else
                val outputFile = outputDir.resolve("data-dump.yaml")
                java.nio.file.Files.writeString(outputFile, jqOutput)
                println(s"Wrote: $outputFile")

            case None if dataBySchema.nonEmpty =>
              dataBySchema.foreach { case (schema, tables) =>
                val output = new StringBuilder()
                if dialect.name == "oracle" then output.append("SET DEFINE OFF\n\n")
                output.append(s"-- Data for schema '$schema'\n\n")
                tables.foreach { case (tableName, _) =>
                  output.append(s"-- Table: $tableName\n")
                  DataExporter.exportTableData(connection, schema, tableName, batchSize, dialect.name).foreach {
                    insertStmt =>
                      output.append(insertStmt)
                      output.append("\n\n")
                  }
                }
                val outputFile = outputDir.resolve(s"$schema-dump.sql")
                val existingContent =
                  if java.nio.file.Files.exists(outputFile) then java.nio.file.Files.readString(outputFile) else ""
                java.nio.file.Files.writeString(outputFile, existingContent + output.toString)
                if existingContent.isEmpty then println(s"Wrote: $outputFile")
                else println(s"Appended data to: $outputFile")
              }

            case None =>

              if tablesBySchema.isEmpty && dataBySchema.isEmpty then
                println("No tables matched the specified patterns.")

        finally connection.close()

  private def parseDataPattern(pattern: String): Either[String, (String, String)] =
    pattern.split("\\.", 2) match
      case Array(schema, tablePattern) if schema.nonEmpty && tablePattern.nonEmpty =>
        Right((schema, tablePattern))
      case _ =>
        Left(s"Invalid data pattern: '$pattern'. Expected format: 'schema.table_pattern'")

  private def sqlLikeToRegex(pattern: String): scala.util.matching.Regex =
    val escaped = pattern
      .replace("\\", "\\\\")
      .replace(".", "\\.")
      .replace("^", "\\^")
      .replace("$", "\\$")
      .replace("[", "\\[")
      .replace("]", "\\]")
      .replace("(", "\\(")
      .replace(")", "\\)")
      .replace("{", "\\{")
      .replace("}", "\\}")
      .replace("|", "\\|")
      .replace("+", "\\+")
      .replace("*", "\\*")
      .replace("%", ".*")
      .replace("_", ".")
    s"(?i)^$escaped$$".r

  private def globToRegex(glob: String): scala.util.matching.Regex =
    val escaped = glob
      .replace(".", "\\.")
      .replace("*", ".*")
      .replace("?", ".")
    s"^$escaped$$".r
