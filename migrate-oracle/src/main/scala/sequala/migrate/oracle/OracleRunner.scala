package sequala.migrate.oracle

import sequala.migrate.{DialectRunner, ExecutionResult, MigrationStep, SchemaDiffer, TransactionMode}
import sequala.oracle.OracleSQL
import sequala.schema.*
import sequala.schema.oracle.*
import sequala.schema.oracle.OracleSqlRenderer.given
import sequala.schema.SqlRenderer.toSql
import java.sql.Connection

/** Oracle-specific diff options with normalization for Oracle's implicit defaults.
  *
  * Normalizes:
  *   - TIMESTAMP(6) ≡ TIMESTAMP (6 is Oracle's fixed default precision)
  *   - NUMBER(10, 0) ≡ INTEGER (Oracle stores INTEGER as NUMBER(10))
  *   - System-generated PK names (SYS_C*) are treated as unnamed
  */
case class OracleDiffOptions(caseInsensitive: Boolean = true, defaultSchema: Option[String] = None) extends DiffOptions:

  override def normalizeDataType(dt: DataType): DataType = dt match
    case SqlTimestamp(Some(6), tz) => SqlTimestamp(None, tz)
    case Number(Some(10), Some(0) | None) => SqlInteger
    // NUMBER(p, 0) ≡ NUMBER(p) - Oracle treats scale 0 as default
    case Number(p, Some(0)) => Number(p, None)
    case other => other

  override def normalizePrimaryKey(pk: PrimaryKey): PrimaryKey =
    pk.name match
      case Some(name) if name.startsWith("SYS_C") => pk.copy(name = None)
      case _ => pk

object OracleDiffOptions:
  val default: OracleDiffOptions = OracleDiffOptions()

/** Oracle implementation of DialectRunner.
  *
  * Handles parsing, schema inspection, diffing, and migration execution for Oracle databases. Uses case-insensitive
  * identifier matching since Oracle stores unquoted identifiers in uppercase.
  */
object OracleRunner extends DialectRunner with OracleDialect:

  val dialectName: String = "oracle"

  def parseSourceDDL(content: String): Either[String, Seq[DialectTable]] =
    if content.trim.isEmpty || content.trim == ";" then Right(Seq.empty)
    else
      val results = OracleSQL.parseAll(content)
      val statements = results.flatMap(_.result.toOption)
      if statements.isEmpty then Left("No valid statements found")
      else Right(OracleSchemaBuilder.fromStatements(statements))

  def inspectSchema(connection: Connection, schema: String): Seq[DialectTable] =
    OracleSchemaInspector.inspectTables(connection, schema)

  def diff(from: Seq[DialectTable], to: Seq[DialectTable], diffOptions: DiffOptions): Seq[SchemaDiffOp] =
    SchemaDiffer.diff(from, to, OracleDiffOptions(caseInsensitive = true, defaultSchema = diffOptions.defaultSchema))

  def renderDiff(diff: SchemaDiffOp): Seq[String] =
    OracleFullSchemaDiffRenderer.render(diff)

  def renderCreateTable(table: DialectTable, pretty: Boolean): String =
    given SqlFormatConfig = if pretty then SqlFormatConfig.Pretty else SqlFormatConfig.Compact
    CreateTable(table).toSql

  def executeMigration(connection: Connection, diffs: Seq[SchemaDiffOp], mode: TransactionMode): ExecutionResult =
    val steps = diffs.map { diff =>
      val comment = describeChange(diff)
      MigrationStep[OracleDataType, OracleColumnOptions, OracleTableOptions](diff, None, comment)
    }
    OracleFullMigrationExecutor.execute(connection, steps, mode)

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
