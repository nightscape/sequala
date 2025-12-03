package sequala.migrate.postgres

import sequala.migrate.{DialectRunner, ExecutionResult, MigrationStep, SchemaDiffer, TransactionMode}
import sequala.postgres.PostgresSQL
import sequala.schema.*
import sequala.schema.postgres.*
import sequala.schema.postgres.PostgresSqlRenderer.given
import sequala.schema.SqlRenderer.toSql
import java.sql.Connection

/** Postgres implementation of DialectRunner.
  *
  * Handles parsing, schema inspection, diffing, and migration execution for PostgreSQL databases.
  */
object PostgresRunner extends DialectRunner with PostgresDialect:

  val dialectName: String = "postgres"

  def parseSourceDDL(content: String): Either[String, Seq[DialectTable]] =
    if content.trim.isEmpty || content.trim == ";" then Right(Seq.empty)
    else
      val results = PostgresSQL.parseAll(content)
      val statements = results.flatMap(_.result.toOption)
      if statements.isEmpty then Left("No valid statements found")
      else Right(PostgresSchemaBuilder.fromStatements(statements))

  def inspectSchema(connection: Connection, schema: String): Seq[DialectTable] =
    PostgresSchemaInspector.inspectTables(connection, schema)

  def diff(from: Seq[DialectTable], to: Seq[DialectTable], diffOptions: DiffOptions): Seq[SchemaDiffOp] =
    SchemaDiffer.diff(from, to, diffOptions)

  def renderDiff(diff: SchemaDiffOp): Seq[String] =
    PostgresSchemaDiffRenderer.render(diff)

  def renderCreateTable(table: DialectTable, pretty: Boolean): String =
    given SqlFormatConfig = if pretty then SqlFormatConfig.Pretty else SqlFormatConfig.Compact
    CreateTable(table).toSql

  def executeMigration(connection: Connection, diffs: Seq[SchemaDiffOp], mode: TransactionMode): ExecutionResult =
    val steps = diffs.map { diff =>
      val comment = describeChange(diff)
      MigrationStep[PostgresDataType, PostgresColumnOptions, PostgresTableOptions](diff, None, comment)
    }
    PostgresMigrationExecutor.execute(connection, steps, mode)

  private def describeChange(diff: SchemaDiffOp): String =
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
