package sequala.migrate.cli

import io.circe.Json
import sequala.common.{Dialect, FilePatternResolver, OracleDialect, SchemaSource}
import sequala.migrate.{DataDiffOp, DataDiffOptions, DataDiffer, DeleteRow, InsertRow, UpdateRow}
import sequala.parse.{
  DirectoryDestination,
  OutputDestination,
  StatementTransform,
  TransformFactory,
  TransformFactoryTokenStore,
  Transforms
}
import sequala.schema.{
  AddColumn,
  AddConstraint,
  AlterTable,
  Column,
  CreateIndex,
  CreateTable,
  DropColumn,
  DropConstraint,
  DropIndex,
  DropTable,
  Insert,
  ModifyColumn,
  RenameColumn,
  SchemaDiffOp,
  SetColumnComment,
  SetTableComment,
  SqlFormatConfig,
  SqlRenderer,
  Statement,
  Table,
  TableConstraint
}
import sequala.schema.ast.Name
import sequala.DbConfig

import java.io.File
import java.sql.{Connection, DriverManager}
import scala.io.Source
import scala.util.{Failure, Success, Try}

object SyncRunner:

  def runSync(
    desired: String,
    db: DbConfig,
    keyColumns: Option[String],
    format: String,
    pretty: Boolean,
    writeTo: OutputDestination,
    sourceTransform: Option[TransformFactory],
    transform: Option[TransformFactory],
    withDeletes: Boolean,
    tableFilter: Option[String],
    ddlsFrom: Option[SchemaSource]
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
        runSyncWithDialect(
          desired,
          dbConfig,
          dialect,
          keyColumns,
          format,
          pretty,
          writeTo,
          sourceTransform,
          transform,
          withDeletes,
          tableFilter,
          transformTokens,
          sourceTransformTokens,
          ddlsFrom
        )

  private def runSyncWithDialect(
    desired: String,
    dbConfig: DatabaseConfig,
    dialect: Dialect,
    keyColumns: Option[String],
    format: String,
    pretty: Boolean,
    writeTo: OutputDestination,
    sourceTransform: Option[TransformFactory],
    transform: Option[TransformFactory],
    withDeletes: Boolean,
    tableFilter: Option[String],
    transformTokens: Option[Seq[String]],
    sourceTransformTokens: Option[Seq[String]],
    ddlsFrom: Option[SchemaSource]
  ): Unit =
    // 1. Parse all desired state files
    val files = FilePatternResolver.resolve(desired)
    if files.isEmpty then
      System.err.println(s"Error: No files match pattern: $desired")
      System.exit(1)
      return

    val allStatements = parseFiles(files, dialect) match
      case Right(stmts) => stmts
      case Left(err) =>
        System.err.println(s"Error: $err")
        System.exit(1)
        return

    val sourceFilePaths = files.map(_.getAbsolutePath)

    // 2. Apply source transform if specified (works on all statement types uniformly)
    val transformedStatements = sourceTransform match
      case Some(factory) =>
        sourceTransformTokens match
          case Some(tokens) =>
            Transforms.resolveTransformsWithWrites(tokens, dialect, sourceFilePaths, pretty) match
              case Right((transforms, writeCollector)) =>
                val transformed = Transforms.apply(allStatements, transforms)
                val writes = writeCollector(allStatements)
                writes.foreach { case (path, content) =>
                  writeTo.write(content, Some(path))
                  println(s"  Wrote: $path")
                }
                transformed
              case Left(err) =>
                System.err.println(s"Error resolving source transforms: $err")
                System.exit(1)
                return
          case None =>
            factory(dialect, sourceFilePaths) match
              case Right(transforms) => Transforms.apply(allStatements, transforms)
              case Left(err) =>
                System.err.println(s"Error applying source transform: $err")
                System.exit(1)
                return
      case None => allStatements

    // 3. Separate into DDL tables and data inserts
    val desiredTables: Seq[dialect.DialectTable] = dialect.tablesFromStatements(transformedStatements)
    val desiredData = DataDiffer.parseInserts(transformedStatements)

    // Build DDL table map for key inference
    val ddlTables: Map[String, Table[?, ?, ?]] = desiredTables.map { t =>
      val key = t.schema match
        case Some(s) => s"${s.toUpperCase}.${t.name.toUpperCase}"
        case None => t.name.toUpperCase
      key -> t.asInstanceOf[Table[?, ?, ?]]
    }.toMap ++ desiredTables.map(t => t.name.toUpperCase -> t.asInstanceOf[Table[?, ?, ?]]).toMap

    // 4. Connect to database
    val connection = createConnection(dbConfig) match
      case Right(conn) => conn
      case Left(err) =>
        System.err.println(s"Error: $err")
        System.exit(1)
        return

    try
      val schemaName = dbConfig.schema
      val dialectName = dialect.name

      // Resolve additional DDL tables from ddlsFrom for PK inference
      val ddlsFromTables: Map[String, Table[?, ?, ?]] = ddlsFrom match
        case Some(source) =>
          source.resolve(dialect, Some(connection), Some(schemaName)) match
            case Right(tables) => tables
            case Left(err) =>
              System.err.println(s"Warning: Failed to resolve --ddls-from: $err")
              Map.empty
        case None => Map.empty

      // Merge ddlTables with ddlsFrom tables (ddlsFrom takes precedence for PK info)
      val allDdlTables = ddlTables ++ ddlsFromTables

      // Resolve schemas to process
      val tablesBySchema: Map[Option[String], Seq[dialect.DialectTable]] = desiredTables.groupBy(_.schema)
      val schemasToProcess = tablesBySchema.keys.flatten.toSeq.distinct match
        case Seq() => resolveSchemas(connection, schemaName, dialect)
        case schemas => schemas

      // 5. Get current state from database (schema + data)
      val schemaDiffs: Seq[Statement] = schemasToProcess.flatMap { schema =>
        val desiredSchemaTable = tablesBySchema.getOrElse(Some(schema), Seq.empty) ++
          tablesBySchema.getOrElse(None, Seq.empty)
        val currentTables = dialect.inspectSchema(connection, schema)
        val diffs = dialect.diffSchemas(currentTables, desiredSchemaTable, schema)

        // Filter if not generating deletes
        if withDeletes then diffs.collect { case s: Statement => s }
        else
          diffs.collect {
            case s: Statement if !s.isInstanceOf[DropTable[?]] && !s.isInstanceOf[DropIndex] => s
          }
      }

      // 6. Compute data diffs (per-table with fallback to original inserts on error)
      val dataDiffs: Seq[Statement] = if desiredData.nonEmpty then
        val tableNames = desiredData.keys.toSeq
        val filteredTableNames = tableFilter match
          case Some(pattern) =>
            val regex = pattern.replace("%", ".*").replace("_", ".").r
            tableNames.filter(name => regex.matches(name))
          case None => tableNames

        val options =
          DataDiffOptions(keyColumns = keyColumns.map(_.split(",").toSeq.map(_.trim)), generateDeletes = withDeletes)

        // Diff each table separately to handle errors gracefully
        filteredTableNames.flatMap { tableName =>
          val parts = tableName.split("\\.")
          val (schema, table) = if parts.length > 1 then (parts(0), parts(1)) else (schemaName, parts(0))
          val currentData = Try(DataExporter.exportAsInserts(connection, schema, table, dialectName)).toOption
            .getOrElse(Seq.empty)
          val currentDataMap = DataDiffer.parseInserts(currentData)
          val tableDesiredData = Map(tableName -> desiredData(tableName))

          DataDiffer.diff(currentDataMap, tableDesiredData, options, allDdlTables) match
            case Right(diffs) => diffs.collect { case s: Statement => s }
            case Left(err) =>
              // If diff fails (e.g., no PK), fall back to original INSERT statements
              System.err.println(s"Warning: Data diff error for $tableName: $err - using original inserts")
              desiredData(tableName).rows.map { row =>
                // Convert Map[String, Expression] to Seq[Expression] ordered by column names
                val valueSeq = row.columns.map(col => row.values(col.name))
                Insert(row.table, Some(row.columns), sequala.schema.ast.ExplicitInsert(Seq(valueSeq)))
              }
        }
      else Seq.empty

      // 7. Combine all operations (DDL first, then data)
      val allOps = orderOperations(schemaDiffs ++ dataDiffs)

      // 8. Apply output transform if specified
      val finalOps = transform match
        case Some(factory) =>
          transformTokens match
            case Some(tokens) =>
              Transforms.resolveTransformsWithWrites(tokens, dialect, Seq.empty, pretty) match
                case Right((transforms, writeCollector)) =>
                  val transformed = Transforms.apply(allOps, transforms)
                  val writes = writeCollector(transformed)
                  writes.foreach { case (path, content) =>
                    writeTo.write(content, Some(path))
                    println(s"  Wrote: $path")
                  }
                  transformed
                case Left(err) =>
                  System.err.println(s"Error resolving transforms: $err")
                  allOps
            case None =>
              factory(dialect, Seq.empty) match
                case Right(transforms) => Transforms.apply(allOps, transforms)
                case Left(err) =>
                  System.err.println(s"Error applying transform: $err")
                  allOps
        case None => allOps

      // 9. Format and output
      if finalOps.isEmpty then writeTo.write("-- No changes detected. Schema and data are in sync.")
      else
        writeTo match
          case DirectoryDestination(baseDir) =>
            java.nio.file.Files.createDirectories(java.nio.file.Paths.get(baseDir))
            val opsBySchema = groupBySchema(finalOps)
            opsBySchema.foreach { case (schema, ops) =>
              if ops.nonEmpty then
                val content = formatOutput(ops, format, pretty, dialect)
                val outputPath = java.nio.file.Paths.get(baseDir, s"$schema-sync.sql")
                java.nio.file.Files.writeString(outputPath, content)
                println(s"  $schema: ${ops.size} statements → $outputPath")
            }
            println(s"\nTotal: ${finalOps.size} statements across ${opsBySchema.size} schemas")
          case _ =>
            writeTo.write(formatOutput(finalOps, format, pretty, dialect))

    finally connection.close()

  private def parseFiles(files: Seq[File], dialect: Dialect): Either[String, Seq[Statement]] =
    val statements = files.flatMap { file =>
      Try(Source.fromFile(file).mkString) match
        case Failure(e) =>
          System.err.println(s"Warning: Failed to read ${file.getName}: ${e.getMessage}")
          Seq.empty
        case Success(content) =>
          // For Oracle dialect, use comment-aware parsing to capture @KEY: value metadata
          if dialect.name == "oracle" then parseOracleWithComments(content)
          else dialect.parser.parseAll(content).flatMap(_.result.toOption)
    }
    if statements.isEmpty then Left("No valid statements found in source files")
    else Right(statements)

  private def parseOracleWithComments(content: String): Seq[Statement] =
    import sequala.oracle.OracleSQL
    import sequala.common.OracleDialect.parser

    // Parse all statements first
    val allResults = parser.parseAll(content)
    val parsedStatements = allResults.flatMap(_.result.toOption)

    // For CREATE TABLE statements, re-parse with comment attachment
    // We need to find the source text for each statement including preceding comments
    parsedStatements.zipWithIndex.map { case (stmt, idx) =>
      stmt match
        case ct: CreateTable[?, ?, ?] =>
          // Find the position of this statement in the content
          // For simplicity, we'll try to find CREATE TABLE and parse from preceding comments
          val tablePattern = s"""(?s)(?:--[^\\n]*\\n)*\\s*CREATE\\s+TABLE\\s+[^;]+;""".r
          val matches = tablePattern.findAllMatchIn(content).toSeq

          if matches.length > idx then
            val matchText = matches(idx).matched
            OracleSQL.parseCreateTableWithComments(matchText) match
              case Right(ctWithComments) => ctWithComments.asInstanceOf[Statement]
              case Left(_) => stmt
          else stmt
        case other => other
    }

  private def orderOperations(ops: Seq[Statement]): Seq[Statement] =
    def priority(s: Statement): Int = s match
      case _: DropTable[?] => 0
      case _: DropIndex => 1
      case _: AlterTable[?, ?, ?, ?] => 2
      case _: CreateTable[?, ?, ?] => 3
      case _: CreateIndex[?] => 4
      case _: DeleteRow => 5
      case _: UpdateRow => 6
      case _: InsertRow => 7
      case _: Insert => 8
      case _ => 9
    ops.sortBy(priority)

  private def groupBySchema(ops: Seq[Statement]): Map[String, Seq[Statement]] =
    ops.groupBy {
      case ct: CreateTable[?, ?, ?] => ct.table.schema.getOrElse("default")
      case dt: DropTable[?] => extractSchema(dt.tableName).getOrElse("default")
      case at: AlterTable[?, ?, ?, ?] => extractSchema(at.tableName).getOrElse("default")
      case ci: CreateIndex[?] => extractSchema(ci.tableName).getOrElse("default")
      case di: DropIndex => extractSchema(di.name).getOrElse("default")
      case ir: InsertRow => extractSchema(ir.table.name).getOrElse("default")
      case ur: UpdateRow => extractSchema(ur.table.name).getOrElse("default")
      case dr: DeleteRow => extractSchema(dr.table.name).getOrElse("default")
      case ins: Insert => extractSchema(ins.table.name).getOrElse("default")
      case _ => "default"
    }

  private def extractSchema(tableName: String): Option[String] =
    if tableName.contains(".") then Some(tableName.split("\\.").head.replace("\"", ""))
    else None

  private def formatOutput(ops: Seq[Statement], format: String, pretty: Boolean, dialect: Dialect): String =
    given SqlFormatConfig = if pretty then SqlFormatConfig.Pretty else SqlFormatConfig.Compact
    val renderers = dialect.renderers
    import renderers.given

    def renderName(n: Name): String = summon[SqlRenderer[Name]].toSql(n)
    def renderExpr(e: sequala.schema.ast.Expression): String =
      summon[SqlRenderer[sequala.schema.ast.Expression]].toSql(e)

    def renderStatement(stmt: Statement): String = stmt match
      case ir: InsertRow =>
        val colsStr = ir.columns.map(renderName).mkString(", ")
        val valsStr = ir.values.map(renderExpr).mkString(", ")
        s"INSERT INTO ${renderName(ir.table)} ($colsStr) VALUES ($valsStr)"
      case ur: UpdateRow =>
        val setStr = ur.setColumns.map { case (n, e) => s"${renderName(n)} = ${renderExpr(e)}" }.mkString(", ")
        val whereStr = ur.keyColumns.map { case (n, e) => s"${renderName(n)} = ${renderExpr(e)}" }.mkString(" AND ")
        s"UPDATE ${renderName(ur.table)} SET $setStr WHERE $whereStr"
      case dr: DeleteRow =>
        val whereStr = dr.keyColumns.map { case (n, e) => s"${renderName(n)} = ${renderExpr(e)}" }.mkString(" AND ")
        s"DELETE FROM ${renderName(dr.table)} WHERE $whereStr"
      case at: AlterTable[?, ?, ?, ?] =>
        // Render AlterTable directly to avoid complex type matching in generic renderer
        import sequala.schema.GenericSqlRenderer.given_SqlRenderer_TableConstraint
        import sequala.schema.DataType
        val actions = at.actions
          .map { action =>
            action match
              case AddColumn(col, _) =>
                val dt = col.dataType.asInstanceOf[DataType].toSql
                val nullable = if col.nullable then "" else " NOT NULL"
                val default = col.default.map(d => s" DEFAULT $d").getOrElse("")
                s"""ADD ("${col.name}" $dt$default$nullable)"""
              case ModifyColumn(col, _) =>
                val dt = col.dataType.asInstanceOf[DataType].toSql
                val nullable = if col.nullable then " NULL" else " NOT NULL"
                val default = col.default.map(d => s" DEFAULT $d").getOrElse("")
                s"""MODIFY ("${col.name}" $dt$default$nullable)"""
              case DropColumn(name, cascade, _) =>
                s"""DROP COLUMN "$name"${if cascade then " CASCADE CONSTRAINTS" else ""}"""
              case AddConstraint(c, _) =>
                s"ADD ${summon[SqlRenderer[TableConstraint]].toSql(c)}"
              case DropConstraint(name, cascade, _) =>
                s"""DROP CONSTRAINT "$name"${if cascade then " CASCADE" else ""}"""
              case RenameColumn(old, nw, _) =>
                s"""RENAME COLUMN "$old" TO "$nw""""
          }
          .mkString(" ")
        s"""ALTER TABLE "${at.tableName.replace(".", "\".\"")}" $actions"""
      case stc: SetTableComment =>
        val comment = stc.comment.map(c => s"'${c.replace("'", "''")}'").getOrElse("''")
        s"""COMMENT ON TABLE "${stc.tableName.replace(".", "\".\"")}" IS $comment"""
      case scc: SetColumnComment =>
        val comment = scc.comment.map(c => s"'${c.replace("'", "''")}'").getOrElse("''")
        s"""COMMENT ON COLUMN "${scc.tableName.replace(".", "\".\"")}"."${scc.columnName}" IS $comment"""
      case other =>
        summon[SqlRenderer[Statement]].toSql(other)

    format.toLowerCase match
      case "json" =>
        // Output as JSON array of SQL strings
        val sqlStatements = ops.map(renderStatement)
        Json.arr(sqlStatements.map(Json.fromString)*).spaces2
      case _ =>
        val output = new StringBuilder
        var inSchemaSection = true

        ops.foreach { stmt =>
          val isDataOp = stmt.isInstanceOf[DataDiffOp]
          if inSchemaSection && isDataOp then
            if output.nonEmpty then output.append("\n")
            output.append("-- Data Changes\n")
            inSchemaSection = false

          output.append(renderStatement(stmt)).append(";\n")
        }
        output.toString.stripSuffix("\n")

  private def resolveSchemas(connection: Connection, schemaPattern: String, dialect: Dialect): Seq[String] =
    if schemaPattern.contains("%") || schemaPattern.contains("_") then
      dialect match
        case OracleDialect =>
          import sequala.migrate.oracle.OracleSchemaInspector
          OracleSchemaInspector.getSchemaNames(connection, Some(schemaPattern))
        case _ => Seq(schemaPattern)
    else Seq(schemaPattern)

  private def createConnection(dbConfig: DatabaseConfig): Either[String, Connection] =
    dbConfig.inferredDialect match
      case OracleDialect => Try(Class.forName("oracle.jdbc.OracleDriver"))
      case _ => ()

    Try {
      val props = new java.util.Properties()
      dbConfig.user.foreach(props.setProperty("user", _))
      dbConfig.password.foreach(props.setProperty("password", _))
      DriverManager.getConnection(dbConfig.jdbcUrl, props)
    } match
      case Success(conn) => Right(conn)
      case Failure(e) => Left(s"Failed to connect to database: ${e.getMessage}")
