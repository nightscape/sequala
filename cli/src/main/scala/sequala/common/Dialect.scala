package sequala.common

import java.sql.Connection
import sequala.schema.{CommonDataType, NoColumnOptions, NoTableOptions, SchemaDiffOp, Table}
import sequala.migrate.{ExecutionResult, TransactionMode}
import sequala.common.parser.SQLBaseObject
import sequala.common.renderer.ParserSqlRenderers
import sequala.schema.codec.DialectCodecs
import sequala.oracle.OracleSQL
import sequala.migrate.oracle.OracleDiffOptions
import sequala.postgres.PostgresSQL
import sequala.ansi.ANSISQL
import mainargs.TokensReader

trait Dialect extends sequala.schema.DbDialect {
  import java.io.File
  import scala.io.Source
  import scala.util.{Failure, Success, Try}

  type DialectTable = Table[DataType, ColumnOptions, TableOptions]

  def name: String

  // Low-level components (for parsing/codecs use cases)
  def codecs: DialectCodecs
  def parser: SQLBaseObject
  def renderers: ParserSqlRenderers

  // High-level migration operations
  def parseSourceDDL(content: String): Either[String, Seq[DialectTable]]
  def tablesFromStatements(statements: Seq[sequala.schema.Statement]): Seq[DialectTable]
  def inspectSchema(connection: Connection, schemaName: String): Seq[DialectTable]
  def diffSchemas(current: Seq[DialectTable], desired: Seq[DialectTable], schemaName: String): Seq[SchemaDiffOp]
  def renderDiffs(diffs: Seq[SchemaDiffOp]): Seq[String]
  def renderTables(tables: Seq[DialectTable], pretty: Boolean): String
  def executeMigration(connection: Connection, diffs: Seq[SchemaDiffOp], mode: TransactionMode): ExecutionResult

  // File-based parsing
  def parseFile(sourcePath: String): Either[String, Seq[DialectTable]] =
    val file = new File(sourcePath)
    if !file.exists() then Left(s"Source file not found: $sourcePath")
    else
      Try(Source.fromFile(file).mkString) match
        case Failure(e) => Left(s"Failed to read source file: ${e.getMessage}")
        case Success(content) => parseSourceDDL(content)

  // Change description
  def describeChange(diff: SchemaDiffOp): String =
    import sequala.schema.*
    diff match
      case ct: CreateTable[?, ?, ?] => s"Create table ${ct.table.qualifiedName}"
      case dt: DropTable[?] => s"Drop table ${dt.tableName}"
      case at: AlterTable[?, ?, ?, ?] =>
        val actions = at.actions
          .map {
            case _: AddColumn[?, ?] => "add column"
            case _: DropColumn[?, ?] => "drop column"
            case _: ModifyColumn[?, ?] => "modify column"
            case _: RenameColumn[?, ?] => "rename column"
            case _: AddConstraint[?, ?] => "add constraint"
            case _: DropConstraint[?, ?] => "drop constraint"
            case _ => "alter"
          }
          .distinct
          .mkString(", ")
        s"Alter table ${at.tableName}: $actions"
      case ci: CreateIndex[?] => s"Create index ${ci.name}"
      case di: DropIndex => s"Drop index ${di.name}"
      case stc: SetTableComment => s"Set comment on table ${stc.tableName}"
      case scc: SetColumnComment => s"Set comment on column ${scc.tableName}.${scc.columnName}"

  // Migration plan formatting
  def formatMigrationPlan(diffs: Seq[SchemaDiffOp], format: String): String =
    val output = new StringBuilder
    def emit(s: String): Unit = output.append(s).append("\n")

    format.toLowerCase match
      case "sql" =>
        diffs.foreach { diff =>
          val comment = describeChange(diff)
          val sqls = renderDiffs(Seq(diff))
          emit(s"-- $comment")
          sqls.foreach(sql => emit(s"$sql;"))
          emit("")
        }
      case "json" =>
        import io.circe.syntax.*
        import io.circe.generic.semiauto.*
        import io.circe.Encoder

        case class MigrationStepJson(comment: String, sql: Seq[String])
        given Encoder[MigrationStepJson] = deriveEncoder

        val stepsJson = diffs.map { diff =>
          MigrationStepJson(describeChange(diff), renderDiffs(Seq(diff)))
        }
        emit(stepsJson.asJson.spaces2)
      case _ =>
        emit(s"=== Migration Plan (${diffs.length} steps) ===")
        emit("")
        diffs.zipWithIndex.foreach { case (diff, idx) =>
          val comment = describeChange(diff)
          emit(s"Step ${idx + 1}: $comment")
          val sql = renderDiffs(Seq(diff))
          sql.foreach(s => emit(s"  SQL: $s"))
          emit("")
        }

    output.toString.stripSuffix("\n")
}

object OracleDialect extends Dialect with sequala.schema.oracle.OracleDialect {
  import sequala.schema.{CommonDiffOptions, SqlFormatConfig}
  import sequala.migrate.{MigrationStep, SchemaDiffer}
  import sequala.migrate.oracle.{
    OracleMigrationExecutor,
    OracleSchemaBuilder,
    OracleSchemaDiffRenderer,
    OracleSchemaInspector
  }
  import sequala.schema.oracle.OracleSqlRenderer.given
  import sequala.schema.SqlRenderer.toSql
  import sequala.schema.oracle.{OracleColumnComment, OracleTableComment}
  import sequala.schema.ast.Name

  def name = "oracle"
  def codecs = sequala.schema.oracle.codec.OracleCirceCodecs
  def parser = OracleSQL
  def renderers = sequala.oracle.renderer.OracleParserSqlRenderers

  def parseSourceDDL(content: String): Either[String, Seq[DialectTable]] =
    if content.trim.isEmpty || content.trim == ";" then Right(Seq.empty)
    else
      val results = OracleSQL.parseAll(content)
      val statements = results.flatMap(_.result.toOption)
      if statements.isEmpty then Left("No valid statements found")
      else Right(OracleSchemaBuilder.fromStatements(statements))

  def tablesFromStatements(statements: Seq[sequala.schema.Statement]): Seq[DialectTable] =
    OracleSchemaBuilder.fromStatements(statements)

  def inspectSchema(connection: Connection, schemaName: String): Seq[DialectTable] =
    OracleSchemaInspector.inspectTables(connection, schemaName)

  def diffSchemas(current: Seq[DialectTable], desired: Seq[DialectTable], schemaName: String): Seq[SchemaDiffOp] =
    SchemaDiffer.diff(current, desired, OracleDiffOptions(caseInsensitive = true, defaultSchema = Some(schemaName)))

  def renderDiffs(diffs: Seq[SchemaDiffOp]): Seq[String] =
    diffs.flatMap(OracleSchemaDiffRenderer.render)

  def renderTables(tables: Seq[DialectTable], pretty: Boolean): String =
    given SqlFormatConfig = if pretty then SqlFormatConfig.Pretty else SqlFormatConfig.Compact
    val createStatements = tables.map(t => sequala.schema.CreateTable(t).toSql)
    val commentStatements = tables.flatMap { t =>
      val schemaPrefix = t.schema.map(s => s""""$s".""").getOrElse("")
      val qualifiedTableName = s"""$schemaPrefix"${t.name}""""
      val tableComments = t.comment.map { c =>
        OracleTableComment(Name(qualifiedTableName), c).toSql
      }
      val columnComments = t.columns.flatMap { col =>
        col.comment.map { c =>
          OracleColumnComment(Name(s"""$qualifiedTableName."${col.name}""""), c).toSql
        }
      }
      tableComments.toSeq ++ columnComments
    }
    (createStatements ++ commentStatements).mkString(";\n\n") + ";"

  def executeMigration(connection: Connection, diffs: Seq[SchemaDiffOp], mode: TransactionMode): ExecutionResult =
    val steps = diffs.map(diff => MigrationStep(diff.asInstanceOf[SchemaDiffOp], None, ""))
    OracleMigrationExecutor.execute(
      connection,
      steps.asInstanceOf[Seq[MigrationStep[CommonDataType, NoColumnOptions.type, NoTableOptions.type]]],
      mode
    )
}

object PostgresDialect extends Dialect with sequala.schema.postgres.PostgresDialect {
  import sequala.schema.{CommonDiffOptions, SqlFormatConfig}
  import sequala.migrate.{MigrationStep, SchemaDiffer}
  import sequala.migrate.postgres.{
    PostgresMigrationExecutor,
    PostgresSchemaBuilder,
    PostgresSchemaDiffRenderer,
    PostgresSchemaInspector
  }
  import sequala.schema.postgres.PostgresSqlRenderer.given
  import sequala.schema.SqlRenderer.toSql

  def name = "postgres"
  def codecs = sequala.schema.postgres.codec.PostgresCirceCodecs
  def parser = PostgresSQL
  def renderers = ParserSqlRenderers

  def parseSourceDDL(content: String): Either[String, Seq[DialectTable]] =
    if content.trim.isEmpty || content.trim == ";" then Right(Seq.empty)
    else
      val results = PostgresSQL.parseAll(content)
      val statements = results.flatMap(_.result.toOption)
      if statements.isEmpty then Left("No valid statements found")
      else Right(PostgresSchemaBuilder.fromStatements(statements))

  def tablesFromStatements(statements: Seq[sequala.schema.Statement]): Seq[DialectTable] =
    PostgresSchemaBuilder.fromStatements(statements)

  def inspectSchema(connection: Connection, schemaName: String): Seq[DialectTable] =
    PostgresSchemaInspector.inspectTables(connection, schemaName)

  def diffSchemas(current: Seq[DialectTable], desired: Seq[DialectTable], schemaName: String): Seq[SchemaDiffOp] =
    SchemaDiffer.diff(current, desired, CommonDiffOptions(caseInsensitive = false, defaultSchema = Some(schemaName)))

  def renderDiffs(diffs: Seq[SchemaDiffOp]): Seq[String] =
    diffs.flatMap(PostgresSchemaDiffRenderer.render)

  def renderTables(tables: Seq[DialectTable], pretty: Boolean): String =
    given SqlFormatConfig = if pretty then SqlFormatConfig.Pretty else SqlFormatConfig.Compact
    tables.map(t => sequala.schema.CreateTable(t).toSql).mkString(";\n\n") + ";"

  def executeMigration(connection: Connection, diffs: Seq[SchemaDiffOp], mode: TransactionMode): ExecutionResult =
    import sequala.schema.postgres.{PostgresDataType, PostgresColumnOptions, PostgresTableOptions}
    val steps = diffs.map(diff => MigrationStep(diff.asInstanceOf[SchemaDiffOp], None, ""))
    PostgresMigrationExecutor.execute(
      connection,
      steps.asInstanceOf[Seq[MigrationStep[PostgresDataType, PostgresColumnOptions, PostgresTableOptions]]],
      mode
    )
}

object AnsiDialect extends Dialect with sequala.schema.AnsiDialect {
  import sequala.schema.{CommonDiffOptions, SqlFormatConfig}
  import sequala.migrate.{GenericMigrationExecutor, MigrationStep, SchemaDiffer}
  import sequala.converter.AnsiSchemaBuilder
  import sequala.schema.GenericSqlRenderer.given
  import sequala.schema.SqlRenderer.toSql

  def name = "ansi"
  def codecs = sequala.schema.codec.AnsiCirceCodecs
  def parser = ANSISQL
  def renderers = ParserSqlRenderers

  def parseSourceDDL(content: String): Either[String, Seq[DialectTable]] =
    if content.trim.isEmpty || content.trim == ";" then Right(Seq.empty)
    else
      val results = ANSISQL.parseAll(content)
      val statements = results.flatMap(_.result.toOption)
      if statements.isEmpty then Left("No valid statements found")
      else Right(AnsiSchemaBuilder.fromStatements(statements))

  def tablesFromStatements(statements: Seq[sequala.schema.Statement]): Seq[DialectTable] =
    AnsiSchemaBuilder.fromStatements(statements)

  def inspectSchema(connection: Connection, schemaName: String): Seq[DialectTable] =
    Seq.empty // ANSI dialect doesn't support schema inspection

  def diffSchemas(current: Seq[DialectTable], desired: Seq[DialectTable], schemaName: String): Seq[SchemaDiffOp] =
    SchemaDiffer.diff(current, desired, CommonDiffOptions(caseInsensitive = false, defaultSchema = Some(schemaName)))

  def renderDiffs(diffs: Seq[SchemaDiffOp]): Seq[String] =
    diffs.flatMap(sequala.migrate.GenericSchemaDiffRenderer.render)

  def renderTables(tables: Seq[DialectTable], pretty: Boolean): String =
    given SqlFormatConfig = if pretty then SqlFormatConfig.Pretty else SqlFormatConfig.Compact
    tables.map(t => sequala.schema.CreateTable(t).toSql).mkString(";\n\n") + ";"

  def executeMigration(connection: Connection, diffs: Seq[SchemaDiffOp], mode: TransactionMode): ExecutionResult =
    val steps = diffs.map(diff => MigrationStep(diff.asInstanceOf[SchemaDiffOp], None, ""))
    GenericMigrationExecutor.execute(
      connection,
      steps.asInstanceOf[Seq[MigrationStep[CommonDataType, NoColumnOptions.type, NoTableOptions.type]]],
      mode
    )
}

object Dialects {
  val all: Seq[Dialect] = Seq(OracleDialect, PostgresDialect, AnsiDialect)
  val names: Seq[String] = all.map(_.name)
  val byName: Map[String, Dialect] = all.map(d => d.name -> d).toMap
  val validNames: String = names.mkString(", ")

  def fromJdbcUrl(url: String): Dialect =
    val lower = url.toLowerCase
    if lower.startsWith("jdbc:postgresql") then PostgresDialect
    else if lower.startsWith("jdbc:oracle") then OracleDialect
    else AnsiDialect

  def defaultSchema(dialect: Dialect): String = dialect match
    case OracleDialect => "SYSTEM"
    case _ => "public"

  def fromString(s: String): Either[String, Dialect] =
    s.toLowerCase match
      case "postgres" | "postgresql" => Right(PostgresDialect)
      case "oracle" => Right(OracleDialect)
      case "generic" | "ansi" => Right(AnsiDialect)
      case other => Left(s"Unknown dialect: $other. Valid dialects: ${validNames}")
}

given DialectReader: TokensReader.Simple[Dialect] with {
  def shortName = "dialect"
  def read(strs: Seq[String]): Either[String, Dialect] = {
    if strs.isEmpty then {
      Left("Missing dialect name")
    } else {
      Dialects.byName.get(strs.head.toLowerCase) match {
        case Some(dialect) => Right(dialect)
        case None => Left(s"Unknown dialect: ${strs.head}. Valid dialects are: ${Dialects.validNames}")
      }
    }
  }
}

given OptionDialectReader: TokensReader.Simple[Option[Dialect]] with {
  def shortName = "dialect"
  def read(strs: Seq[String]): Either[String, Option[Dialect]] =
    if strs.isEmpty then Right(None)
    else
      Dialects.byName.get(strs.head.toLowerCase) match
        case Some(dialect) => Right(Some(dialect))
        case None => Left(s"Unknown dialect: ${strs.head}. Valid dialects are: ${Dialects.validNames}")
}
