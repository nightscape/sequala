package sequala.migrate.cli

import sequala.ansi.ANSISQL
import sequala.oracle.OracleSQL
import sequala.postgres.PostgresSQL
import sequala.common.parser.SQLBaseObject
import sequala.common.statement.{CreateTableStatement, Statement}
import sequala.converter.SchemaBuilder
import sequala.schema.*
import sequala.schema.postgres.*
import sequala.schema.oracle.*
import sequala.migrate.*
import sequala.migrate.inspect.SchemaInspector
import sequala.migrate.postgres.{PostgresMigrationExecutor, PostgresSchemaDiffRenderer, PostgresSchemaInspector}
import sequala.migrate.oracle.{
  OracleFullMigrationExecutor,
  OracleFullSchemaDiffRenderer,
  OracleMigrationExecutor,
  OracleSchemaDiffRenderer,
  OracleSchemaInspector
}
import com.typesafe.config.ConfigFactory

import java.io.{File, PrintWriter}
import java.sql.{Connection, DriverManager}
import scala.io.{Source, StdIn}
import scala.util.{Failure, Success, Try, Using}
import scala.concurrent.duration.*

object MigrateRunner:

  def runPlan(cmd: PlanCommand): Unit =
    val result = for
      config <- ConfigLoader.loadConfig(cmd.config, cmd.env)
      dbConfig <- ConfigLoader.loadDatabaseConfig(
        config,
        urlOverride = cmd.database,
        userOverride = cmd.user,
        passwordOverride = cmd.password,
        schemaOverride = cmd.schema,
        dialectOverride = cmd.dialect
      )
      desiredTables <- parseSourceDDL(cmd.source, dbConfig.inferredDialect)
      connection <- createConnection(dbConfig)
      currentTables <- inspectCurrentSchema(connection, dbConfig)
      _ = connection.close()
      diffs = computeDiff(currentTables, desiredTables, dbConfig.inferredDialect)
      steps = generateMigrationSteps(diffs, dbConfig.inferredDialect)
    yield (steps, dbConfig)

    result match
      case Left(error) =>
        System.err.println(s"Error: $error")
        System.exit(1)
      case Right((steps, dbConfig)) =>
        if steps.isEmpty then println("No changes detected. Schema is up to date.")
        else outputMigrationPlan(steps, dbConfig.inferredDialect, cmd.format)

  def runApply(cmd: ApplyCommand): Unit =
    val result = for
      config <- ConfigLoader.loadConfig(cmd.config, cmd.env)
      dbConfig <- ConfigLoader.loadDatabaseConfig(
        config,
        urlOverride = cmd.database,
        userOverride = cmd.user,
        passwordOverride = cmd.password,
        schemaOverride = cmd.schema,
        dialectOverride = cmd.dialect
      )
      desiredTables <- parseSourceDDL(cmd.source, dbConfig.inferredDialect)
      connection <- createConnection(dbConfig)
      currentTables <- inspectCurrentSchema(connection, dbConfig)
      diffs = computeDiff(currentTables, desiredTables, dbConfig.inferredDialect)
      steps = generateMigrationSteps(diffs, dbConfig.inferredDialect)
    yield (steps, dbConfig, connection)

    result match
      case Left(error) =>
        System.err.println(s"Error: $error")
        System.exit(1)
      case Right((steps, dbConfig, connection)) =>
        try
          if steps.isEmpty then println("No changes detected. Schema is up to date.")
          else if cmd.dryRun then
            println("=== DRY RUN MODE ===")
            outputMigrationPlan(steps, dbConfig.inferredDialect, "sql")
            println("\nNo changes were applied (dry run mode).")
          else
            outputMigrationPlan(steps, dbConfig.inferredDialect, "text")
            println()

            val proceed = cmd.autoApprove || promptConfirmation()
            if proceed then
              println("\nApplying migration...")
              val execResult = executeMigration(connection, steps, dbConfig.inferredDialect, cmd.transactional)
              execResult match
                case Left(error) =>
                  System.err.println(s"Migration failed: $error")
                  System.exit(1)
                case Right(result) =>
                  if result.successful then
                    println(s"\n✓ Migration completed successfully!")
                    println(s"  Executed ${result.executedCount} steps in ${formatDuration(result.totalExecutionTime)}")
                  else
                    System.err.println(s"\n✗ Migration failed!")
                    result.failedStep.foreach { step =>
                      System.err.println(
                        s"  Failed at step ${step.stepIndex}: ${step.error.map(_.getMessage).getOrElse("Unknown error")}"
                      )
                      System.err.println(s"  SQL: ${step.sql}")
                    }
                    System.exit(1)
            else println("Migration cancelled.")
        finally
          connection.close()

  def runApplyTestable(cmd: ApplyCommand): Unit =
    val result = for
      config <- ConfigLoader.loadConfig(cmd.config, cmd.env)
      dbConfig <- ConfigLoader.loadDatabaseConfig(
        config,
        urlOverride = cmd.database,
        userOverride = cmd.user,
        passwordOverride = cmd.password,
        schemaOverride = cmd.schema,
        dialectOverride = cmd.dialect
      )
      desiredTables <- parseSourceDDL(cmd.source, dbConfig.inferredDialect)
      connection <- createConnection(dbConfig)
      currentTables <- inspectCurrentSchema(connection, dbConfig)
      diffs = computeDiff(currentTables, desiredTables, dbConfig.inferredDialect)
      steps = generateMigrationSteps(diffs, dbConfig.inferredDialect)
    yield (steps, dbConfig, connection)

    result match
      case Left(error) =>
        throw new RuntimeException(s"Migration failed: $error")
      case Right((steps, dbConfig, connection)) =>
        try
          if steps.nonEmpty && !cmd.dryRun then
            val execResult = executeMigration(connection, steps, dbConfig.inferredDialect, cmd.transactional)
            execResult match
              case Left(error) =>
                throw new RuntimeException(s"Migration execution failed: $error")
              case Right(result) =>
                if !result.successful then
                  val msg = result.failedStep
                    .map(s => s"Failed at step ${s.stepIndex}: ${s.error.map(_.getMessage).getOrElse("Unknown")}")
                    .getOrElse("Unknown failure")
                  throw new RuntimeException(msg)
        finally
          connection.close()

  def runInspect(cmd: InspectCommand): Unit =
    val result = for
      config <- ConfigLoader.loadConfig(cmd.config, cmd.env)
      dbConfig <- ConfigLoader.loadDatabaseConfig(
        config,
        urlOverride = cmd.database,
        userOverride = cmd.user,
        passwordOverride = cmd.password,
        schemaOverride = cmd.schema,
        dialectOverride = cmd.dialect
      )
      connection <- createConnection(dbConfig)
      tables <- inspectCurrentSchema(connection, dbConfig)
      _ = connection.close()
    yield (tables, dbConfig)

    result match
      case Left(error) =>
        System.err.println(s"Error: $error")
        System.exit(1)
      case Right((tables, dbConfig)) =>
        val filteredTables = cmd.tableFilter match
          case Some(pattern) =>
            val regex = globToRegex(pattern)
            tables.filter { t =>
              val name = getTableName(t, dbConfig.inferredDialect)
              regex.matches(name) || regex.matches(name.toLowerCase)
            }
          case None => tables

        val ddl = generateDDL(filteredTables, dbConfig.inferredDialect, cmd.pretty)

        cmd.output match
          case Some(outputPath) =>
            Using(new PrintWriter(new File(outputPath))) { writer =>
              writer.println(ddl)
            } match
              case Success(_) => println(s"Schema written to $outputPath")
              case Failure(e) =>
                System.err.println(s"Failed to write output file: ${e.getMessage}")
                System.exit(1)
          case None =>
            println(ddl)

  def runDiff(cmd: DiffCommand): Unit =
    val dialect = DatabaseDialect.fromString(cmd.dialect) match
      case Right(d) => d
      case Left(err) =>
        System.err.println(s"Error: $err")
        System.exit(1)
        return

    val result = for
      fromTables <- parseSourceDDL(cmd.from, dialect)
      toTables <- parseSourceDDL(cmd.to, dialect)
      diffs = computeDiff(fromTables, toTables, dialect)
      steps = generateMigrationSteps(diffs, dialect)
    yield steps

    result match
      case Left(error) =>
        System.err.println(s"Error: $error")
        System.exit(1)
      case Right(steps) =>
        if steps.isEmpty then println("No differences found between schemas.")
        else outputMigrationPlan(steps, dialect, cmd.format)

  private def parseSourceDDL(sourcePath: String, dialect: DatabaseDialect): Either[String, Seq[AnyTable]] =
    val file = new File(sourcePath)
    if !file.exists() then return Left(s"Source file not found: $sourcePath")

    Try(Source.fromFile(file).mkString) match
      case Failure(e) => Left(s"Failed to read source file: ${e.getMessage}")
      case Success(content) =>
        val trimmedContent = content.trim
        if trimmedContent.isEmpty || trimmedContent == ";" then Right(Seq.empty)
        else
          val parser = dialectToParser(dialect)
          val parseResults = parser.parseAll(content)
          val statements = parseResults.flatMap(_.result.toOption)

          if statements.isEmpty then Left(s"No valid statements found in $sourcePath")
          else
            val tables = SchemaBuilder.fromStatements(statements)
            Right(tables.map(t => AnyTable.Generic(t)))

  private def dialectToParser(dialect: DatabaseDialect): SQLBaseObject =
    dialect match
      case DatabaseDialect.Postgres => PostgresSQL
      case DatabaseDialect.Oracle => OracleSQL
      case DatabaseDialect.Generic => ANSISQL

  private def createConnection(dbConfig: DatabaseConfig): Either[String, Connection] =
    dbConfig.inferredDialect match
      case DatabaseDialect.Oracle =>
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

  private def inspectCurrentSchema(connection: Connection, dbConfig: DatabaseConfig): Either[String, Seq[AnyTable]] =
    Try {
      dbConfig.inferredDialect match
        case DatabaseDialect.Postgres =>
          val tables = PostgresSchemaInspector.inspectTables(connection, dbConfig.schema)
          tables.map(t => AnyTable.Postgres(t))
        case DatabaseDialect.Oracle =>
          val tables = OracleSchemaInspector.inspectTables(connection, dbConfig.schema)
          tables.map(t => AnyTable.Oracle(t))
        case DatabaseDialect.Generic =>
          val tables = PostgresSchemaInspector.inspectTables(connection, dbConfig.schema)
          tables.map(t => AnyTable.Postgres(t))
    } match
      case Success(tables) => Right(tables)
      case Failure(e) => Left(s"Failed to inspect database schema: ${e.getMessage}")

  private def computeDiff(from: Seq[AnyTable], to: Seq[AnyTable], dialect: DatabaseDialect): Seq[AnySchemaDiff] =
    (dialect, from.headOption, to.headOption) match
      case (DatabaseDialect.Postgres, _, _) =>
        val fromPg = from.flatMap(_.toPostgres)
        val toPg = to.map(_.toPostgresOrConvert)
        SchemaDiffer.diff(fromPg, toPg).map(d => AnySchemaDiff.Postgres(d))
      case (DatabaseDialect.Oracle, _, _) =>
        val fromOracle = from.flatMap(_.toOracle)
        val toOracle = to.map(_.toOracleOrConvert)
        SchemaDiffer.diff(fromOracle, toOracle).map(d => AnySchemaDiff.Oracle(d))
      case (DatabaseDialect.Generic, _, _) =>
        val fromGeneric = from.map(_.toGenericOrConvert)
        val toGeneric = to.map(_.toGenericOrConvert)
        SchemaDiffer.diff(fromGeneric, toGeneric).map(d => AnySchemaDiff.Generic(d))

  private def generateMigrationSteps(diffs: Seq[AnySchemaDiff], dialect: DatabaseDialect): Seq[AnyMigrationStep] =
    diffs.map { diff =>
      val comment = diff match
        case AnySchemaDiff.Postgres(d) => describeChange(d)
        case AnySchemaDiff.Oracle(d) => describeChange(d)
        case AnySchemaDiff.Generic(d) => describeChange(d)
      AnyMigrationStep(diff, comment)
    }

  private def describeChange[DT <: DataType, CO <: ColumnOptions, TO <: TableOptions](
    diff: SchemaDiff[DT, CO, TO]
  ): String =
    diff match
      case ct: CreateTable[_, _, _] => s"Create table ${ct.table.name}"
      case dt: DropTable => s"Drop table ${dt.tableName}"
      case at: AlterTable[_, _, _, _] =>
        val actions = at.actions
          .map {
            case _: AddColumn[_, _] => "add column"
            case _: DropColumn[_, _] => "drop column"
            case _: ModifyColumn[_, _] => "modify column"
            case _: RenameColumn[_, _] => "rename column"
            case _: AddConstraint[_, _] => "add constraint"
            case _: DropConstraint[_, _] => "drop constraint"
            case _ => "alter"
          }
          .distinct
          .mkString(", ")
        s"Alter table ${at.tableName}: $actions"
      case ci: CreateIndex => s"Create index ${ci.name}"
      case di: DropIndex => s"Drop index ${di.name}"

  private def outputMigrationPlan(steps: Seq[AnyMigrationStep], dialect: DatabaseDialect, format: String): Unit =
    format.toLowerCase match
      case "sql" =>
        steps.foreach { step =>
          val sql = renderStepToSql(step, dialect)
          sql.foreach { s =>
            println(s"-- ${step.comment}")
            println(s"$s;")
            println()
          }
        }
      case "json" =>
        println("{")
        println("  \"steps\": [")
        steps.zipWithIndex.foreach { case (step, idx) =>
          val sql = renderStepToSql(step, dialect)
          val sqlJson = sql.map(s => s""""${escapeJson(s)}"""").mkString(", ")
          val comma = if idx < steps.length - 1 then "," else ""
          println(s"""    {"comment": "${escapeJson(step.comment)}", "sql": [$sqlJson]}$comma""")
        }
        println("  ]")
        println("}")
      case _ =>
        println(s"=== Migration Plan (${steps.length} steps) ===")
        println()
        steps.zipWithIndex.foreach { case (step, idx) =>
          println(s"Step ${idx + 1}: ${step.comment}")
          val sql = renderStepToSql(step, dialect)
          sql.foreach(s => println(s"  SQL: $s"))
          println()
        }

  private def renderStepToSql(step: AnyMigrationStep, dialect: DatabaseDialect): Seq[String] =
    step.diff match
      case AnySchemaDiff.Postgres(d) => PostgresSchemaDiffRenderer.render(d)
      case AnySchemaDiff.Oracle(d) => OracleFullSchemaDiffRenderer.render(d)
      case AnySchemaDiff.Generic(d) => GenericSchemaDiffRenderer.render(d)

  private def executeMigration(
    connection: Connection,
    steps: Seq[AnyMigrationStep],
    dialect: DatabaseDialect,
    transactional: Boolean
  ): Either[String, ExecutionResult] =
    val mode = if transactional then TransactionMode.Transactional else TransactionMode.AutoCommit

    Try {
      dialect match
        case DatabaseDialect.Postgres =>
          val pgSteps = steps.map { step =>
            val diff = step.diff match
              case AnySchemaDiff.Postgres(d) => d
              case other =>
                val generic = other match
                  case AnySchemaDiff.Generic(d) => d
                  case AnySchemaDiff.Oracle(d) => throw new RuntimeException("Cannot convert Oracle diff to Postgres")
                  case AnySchemaDiff.Postgres(d) => throw new RuntimeException("Unexpected")
                convertGenericDiffToPostgres(generic)
            MigrationStep(diff, None, step.comment)
          }
          PostgresMigrationExecutor.execute(connection, pgSteps, mode)

        case DatabaseDialect.Oracle =>
          val oracleSteps = steps.map { step =>
            step.diff match
              case AnySchemaDiff.Oracle(d) =>
                MigrationStep[OracleDataType, OracleColumnOptions, OracleTableOptions](d, None, step.comment)
              case AnySchemaDiff.Generic(d) =>
                MigrationStep[CommonDataType, NoColumnOptions.type, NoTableOptions.type](d, None, step.comment)
              case AnySchemaDiff.Postgres(_) => throw new RuntimeException("Cannot convert Postgres diff to Oracle")
          }
          val hasOracleTyped = steps.exists { s =>
            s.diff match
              case AnySchemaDiff.Oracle(_) => true
              case _ => false
          }
          if hasOracleTyped then
            val typedSteps = oracleSteps.collect {
              case s: MigrationStep[OracleDataType, OracleColumnOptions, OracleTableOptions] @unchecked
                  if s.statement.isInstanceOf[CreateTable[_, _, _]] ||
                    s.statement.isInstanceOf[AlterTable[_, _, _, _]] =>
                s
            }
            OracleFullMigrationExecutor.execute(connection, typedSteps, mode)
          else
            val genericSteps = steps.map { step =>
              val diff = step.diff match
                case AnySchemaDiff.Generic(d) => d
                case _ => throw new RuntimeException("Expected generic diff for Oracle")
              MigrationStep(diff, None, step.comment)
            }
            OracleMigrationExecutor.execute(connection, genericSteps, mode)

        case DatabaseDialect.Generic =>
          val genericSteps = steps.map { step =>
            val diff = step.diff match
              case AnySchemaDiff.Generic(d) => d
              case _ => throw new RuntimeException("Expected generic diff")
            MigrationStep(diff, None, step.comment)
          }
          GenericMigrationExecutor.execute(connection, genericSteps, mode)
    } match
      case Success(result) => Right(result)
      case Failure(e) => Left(e.getMessage)

  private def convertGenericDiffToPostgres(
    diff: SchemaDiff[CommonDataType, NoColumnOptions.type, NoTableOptions.type]
  ): SchemaDiff[PostgresDataType, PostgresColumnOptions, PostgresTableOptions] =
    diff match
      case ct: CreateTable[CommonDataType, NoColumnOptions.type, NoTableOptions.type] @unchecked =>
        val pgTable = convertGenericTableToPostgres(ct.table)
        CreateTable(pgTable, ct.orReplace, ct.ifNotExists)
      case dt: DropTable => dt
      case ci: CreateIndex => ci
      case di: DropIndex => di
      case at: AlterTable[
            CommonDataType,
            NoColumnOptions.type,
            NoTableOptions.type,
            AlterTableAction[CommonDataType, NoColumnOptions.type]
          ] @unchecked =>
        val pgActions = at.actions.map { action =>
          convertGenericActionToPostgres(action.asInstanceOf[AlterTableAction[CommonDataType, NoColumnOptions.type]])
        }
        AlterTable[PostgresDataType, PostgresColumnOptions, PostgresTableOptions, PostgresAlterTableAction](
          at.tableName,
          pgActions
        )

  private def convertGenericTableToPostgres(
    table: Table[CommonDataType, NoColumnOptions.type, NoTableOptions.type]
  ): Table[PostgresDataType, PostgresColumnOptions, PostgresTableOptions] =
    Table(
      name = table.name,
      columns = table.columns.map(c => Column(c.name, c.dataType, c.nullable, c.default, PostgresColumnOptions.empty)),
      primaryKey = table.primaryKey,
      indexes = table.indexes,
      foreignKeys = table.foreignKeys,
      checks = table.checks,
      uniques = table.uniques,
      options = PostgresTableOptions.empty
    )

  private def convertGenericActionToPostgres(
    action: AlterTableAction[CommonDataType, NoColumnOptions.type]
  ): PostgresAlterTableAction =
    action match
      case AddColumn(col) =>
        AddColumn(Column(col.name, col.dataType, col.nullable, col.default, PostgresColumnOptions.empty))
      case DropColumn(name, cascade) => DropColumn(name, cascade)
      case ModifyColumn(col) =>
        ModifyColumn(Column(col.name, col.dataType, col.nullable, col.default, PostgresColumnOptions.empty))
      case RenameColumn(oldName, newName) => RenameColumn(oldName, newName)
      case AddConstraint(constraint) => AddConstraint(constraint)
      case DropConstraint(name, cascade) => DropConstraint(name, cascade)

  private def generateDDL(tables: Seq[AnyTable], dialect: DatabaseDialect, pretty: Boolean = false): String =
    import sequala.schema.SqlRenderer.toSql
    given SqlFormatConfig = if pretty then SqlFormatConfig.Pretty else SqlFormatConfig.Compact

    tables
      .map { anyTable =>
        dialect match
          case DatabaseDialect.Postgres =>
            anyTable match
              case AnyTable.Postgres(t) =>
                import sequala.schema.postgres.PostgresSqlRenderer.given
                CreateTable(t).toSql
              case AnyTable.Generic(t) =>
                import sequala.schema.GenericSqlRenderer.given
                CreateTable(t).toSql
              case AnyTable.Oracle(t) =>
                import sequala.schema.oracle.OracleSqlRenderer.given
                CreateTable(t).toSql
          case DatabaseDialect.Oracle =>
            anyTable match
              case AnyTable.Oracle(t) =>
                import sequala.schema.oracle.OracleSqlRenderer.given
                CreateTable(t).toSql
              case AnyTable.Generic(t) =>
                import sequala.schema.oracle.OracleSqlRenderer.given
                CreateTable(t).toSql
              case AnyTable.Postgres(t) =>
                import sequala.schema.postgres.PostgresSqlRenderer.given
                CreateTable(t).toSql
          case DatabaseDialect.Generic =>
            anyTable match
              case AnyTable.Generic(t) =>
                import sequala.schema.GenericSqlRenderer.given
                CreateTable(t).toSql
              case AnyTable.Postgres(t) =>
                import sequala.schema.postgres.PostgresSqlRenderer.given
                CreateTable(t).toSql
              case AnyTable.Oracle(t) =>
                import sequala.schema.oracle.OracleSqlRenderer.given
                CreateTable(t).toSql
      }
      .mkString(";\n\n") + ";"

  private def getTableName(table: AnyTable, dialect: DatabaseDialect): String =
    table match
      case AnyTable.Postgres(t) => t.name
      case AnyTable.Oracle(t) => t.name
      case AnyTable.Generic(t) => t.name

  private def promptConfirmation(): Boolean =
    print("Do you want to apply these changes? [y/N] ")
    Console.flush()
    val input = StdIn.readLine()
    input != null && input.trim.toLowerCase == "y"

  private def formatDuration(d: Duration): String =
    if d.toMillis < 1000 then s"${d.toMillis}ms"
    else if d.toSeconds < 60 then f"${d.toMillis / 1000.0}%.2fs"
    else s"${d.toMinutes}m ${d.toSeconds % 60}s"

  private def globToRegex(glob: String): scala.util.matching.Regex =
    val escaped = glob
      .replace(".", "\\.")
      .replace("*", ".*")
      .replace("?", ".")
    s"^$escaped$$".r

  private def escapeJson(s: String): String =
    s.replace("\\", "\\\\")
      .replace("\"", "\\\"")
      .replace("\n", "\\n")
      .replace("\r", "\\r")
      .replace("\t", "\\t")

enum AnyTable:
  case Postgres(table: Table[PostgresDataType, PostgresColumnOptions, PostgresTableOptions])
  case Oracle(table: Table[OracleDataType, OracleColumnOptions, OracleTableOptions])
  case Generic(table: Table[CommonDataType, NoColumnOptions.type, NoTableOptions.type])

  def toPostgres: Option[Table[PostgresDataType, PostgresColumnOptions, PostgresTableOptions]] = this match
    case Postgres(t) => Some(t)
    case _ => None

  def toOracle: Option[Table[OracleDataType, OracleColumnOptions, OracleTableOptions]] = this match
    case Oracle(t) => Some(t)
    case _ => None

  def toGeneric: Option[Table[CommonDataType, NoColumnOptions.type, NoTableOptions.type]] = this match
    case Generic(t) => Some(t)
    case _ => None

  def toPostgresOrConvert: Table[PostgresDataType, PostgresColumnOptions, PostgresTableOptions] = this match
    case Postgres(t) => t
    case Generic(t) =>
      Table(
        name = t.name,
        columns = t.columns.map(c => Column(c.name, c.dataType, c.nullable, c.default, PostgresColumnOptions.empty)),
        primaryKey = t.primaryKey,
        indexes = t.indexes,
        foreignKeys = t.foreignKeys,
        checks = t.checks,
        uniques = t.uniques,
        options = PostgresTableOptions.empty
      )
    case Oracle(t) =>
      Table(
        name = t.name,
        columns = t.columns.map(c =>
          Column(c.name, convertOracleToCommon(c.dataType), c.nullable, c.default, PostgresColumnOptions.empty)
        ),
        primaryKey = t.primaryKey,
        indexes = t.indexes,
        foreignKeys = t.foreignKeys,
        checks = t.checks,
        uniques = t.uniques,
        options = PostgresTableOptions.empty
      )

  def toOracleOrConvert: Table[OracleDataType, OracleColumnOptions, OracleTableOptions] = this match
    case Oracle(t) => t
    case Generic(t) =>
      Table(
        name = t.name,
        columns = t.columns.map(c => Column(c.name, c.dataType, c.nullable, c.default, OracleColumnOptions.empty)),
        primaryKey = t.primaryKey,
        indexes = t.indexes,
        foreignKeys = t.foreignKeys,
        checks = t.checks,
        uniques = t.uniques,
        options = OracleTableOptions.empty
      )
    case Postgres(t) =>
      Table(
        name = t.name,
        columns = t.columns.map(c =>
          Column(c.name, convertPostgresToCommon(c.dataType), c.nullable, c.default, OracleColumnOptions.empty)
        ),
        primaryKey = t.primaryKey,
        indexes = t.indexes,
        foreignKeys = t.foreignKeys,
        checks = t.checks,
        uniques = t.uniques,
        options = OracleTableOptions.empty
      )

  def toGenericOrConvert: Table[CommonDataType, NoColumnOptions.type, NoTableOptions.type] = this match
    case Generic(t) => t
    case Postgres(t) =>
      Table(
        name = t.name,
        columns = t.columns.map(c =>
          Column(c.name, convertPostgresToCommon(c.dataType), c.nullable, c.default, NoColumnOptions)
        ),
        primaryKey = t.primaryKey,
        indexes = t.indexes,
        foreignKeys = t.foreignKeys,
        checks = t.checks,
        uniques = t.uniques,
        options = NoTableOptions
      )
    case Oracle(t) =>
      Table(
        name = t.name,
        columns =
          t.columns.map(c => Column(c.name, convertOracleToCommon(c.dataType), c.nullable, c.default, NoColumnOptions)),
        primaryKey = t.primaryKey,
        indexes = t.indexes,
        foreignKeys = t.foreignKeys,
        checks = t.checks,
        uniques = t.uniques,
        options = NoTableOptions
      )

  private def convertPostgresToCommon(dt: PostgresDataType): CommonDataType =
    dt match
      case c: CommonDataType => c
      case Serial => SqlInteger
      case BigSerial => SqlBigInt
      case SmallSerial => SmallInt
      case Uuid => VarChar(36)
      case Json => SqlText
      case Jsonb => SqlText
      case Bytea => SqlBlob
      case Inet => VarChar(45)
      case Cidr => VarChar(45)
      case MacAddr => VarChar(17)
      case MacAddr8 => VarChar(23)
      case Money => Decimal(19, 4)
      case Bit(len) => VarChar(len.getOrElse(1))
      case BitVarying(len) => VarChar(len.getOrElse(255))
      case PgPoint => VarChar(100)
      case PgLine => VarChar(100)
      case PgBox => VarChar(100)
      case PgCircle => VarChar(100)
      case PgPolygon => SqlText
      case PgPath => SqlText
      case TsVector => SqlText
      case TsQuery => SqlText
      case PgArray(_) => SqlText

  private def convertOracleToCommon(dt: OracleDataType): CommonDataType =
    dt match
      case c: CommonDataType => c
      case Varchar2(len, _) => VarChar(len)
      case NVarchar2(len) => VarChar(len)
      case OracleChar(len, _) => SqlChar(len)
      case NChar(len) => SqlChar(len)
      case Number(prec, scale) =>
        (prec, scale) match
          case (Some(p), Some(0)) if p <= 10 => SqlInteger
          case (Some(p), Some(0)) if p <= 19 => SqlBigInt
          case (Some(p), Some(s)) => Decimal(p, s)
          case (Some(p), None) => Decimal(p, 0)
          case (None, _) => Decimal(38, 0)
      case BinaryFloat => Real
      case BinaryDouble => DoublePrecision
      case Raw(_) => SqlBlob
      case OracleLong => SqlClob
      case LongRaw => SqlBlob
      case OracleRowid => VarChar(18)
      case URowid(len) => VarChar(len.getOrElse(4000))
      case XMLType => SqlClob
      case IntervalYearToMonth(_) => VarChar(50)
      case IntervalDayToSecond(_, _) => VarChar(50)

enum AnySchemaDiff:
  case Postgres(diff: SchemaDiff[PostgresDataType, PostgresColumnOptions, PostgresTableOptions])
  case Oracle(diff: SchemaDiff[OracleDataType, OracleColumnOptions, OracleTableOptions])
  case Generic(diff: SchemaDiff[CommonDataType, NoColumnOptions.type, NoTableOptions.type])

case class AnyMigrationStep(diff: AnySchemaDiff, comment: String)

object GenericSchemaDiffRenderer extends SchemaDiffRenderer[CommonDataType, NoColumnOptions.type, NoTableOptions.type]:
  import sequala.schema.GenericSqlRenderer.given
  import sequala.schema.SqlRenderer.{toSql, given}

  def renderCreateTable(ct: CreateTable[CommonDataType, NoColumnOptions.type, NoTableOptions.type]): String =
    ct.toSql

  def renderDropTable(dt: DropTable): String = dt.toSql

  def renderAlterTable(
    at: AlterTable[
      CommonDataType,
      NoColumnOptions.type,
      NoTableOptions.type,
      AlterTableAction[CommonDataType, NoColumnOptions.type]
    ]
  ): Seq[String] =
    at.actions.map { action =>
      val singleActionAt =
        AlterTable[CommonDataType, NoColumnOptions.type, NoTableOptions.type, GenericAlterTableAction](
          at.tableName,
          Seq(action.asInstanceOf[GenericAlterTableAction])
        )
      singleActionAt.toSql
    }

  def renderCreateIndex(ci: CreateIndex): String = ci.toSql

  def renderDropIndex(di: DropIndex): String = di.toSql

object GenericMigrationExecutor extends MigrationExecutor[CommonDataType, NoColumnOptions.type, NoTableOptions.type]:
  val renderer: SchemaDiffRenderer[CommonDataType, NoColumnOptions.type, NoTableOptions.type] =
    GenericSchemaDiffRenderer

  val supportsTransactionalDDL: Boolean = true
