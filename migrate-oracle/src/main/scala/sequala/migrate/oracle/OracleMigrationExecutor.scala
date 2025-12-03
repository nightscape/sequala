package sequala.migrate.oracle

import sequala.migrate.*
import sequala.schema.*
import sequala.schema.oracle.*
import sequala.schema.oracle.OracleSqlRenderer.{toOracleSql, given}
import sequala.schema.SqlRenderer.{toSql, given}

/** Renders generic schema operations with Oracle-compatible SQL for data types. */
object OracleSchemaDiffRenderer extends SchemaDiffRenderer[CommonDataType, NoColumnOptions.type, NoTableOptions.type]:

  private def quote(name: String): String = s""""$name""""

  private def renderColumn(col: Column[CommonDataType, NoColumnOptions.type]): String =
    val nullStr = if col.nullable then "" else " NOT NULL"
    val defaultStr = col.default.map(d => s" DEFAULT $d").getOrElse("")
    s"${quote(col.name)} ${toOracleSql(col.dataType)}$defaultStr$nullStr"

  private def renderTable(table: Table[CommonDataType, NoColumnOptions.type, NoTableOptions.type]): String =
    val colDefs = table.columns.map(renderColumn)
    val pkDef = table.primaryKey.map(pk => summon[SqlRenderer[PrimaryKey]].toSql(pk))
    val fkDefs = table.foreignKeys.map(fk => summon[SqlRenderer[ForeignKey]].toSql(fk))
    val uniqueDefs = table.uniques.map(u => summon[SqlRenderer[Unique]].toSql(u))
    val checkDefs = table.checks.map(c => summon[SqlRenderer[Check]].toSql(c))
    val allDefs = colDefs ++ pkDef ++ fkDefs ++ uniqueDefs ++ checkDefs
    s"${quote(table.name)} (${allDefs.mkString(", ")})"

  def renderCreateTable(ct: CreateTable[CommonDataType, NoColumnOptions.type, NoTableOptions.type]): String =
    val orReplaceStr = if ct.orReplace then "OR REPLACE " else ""
    s"CREATE ${orReplaceStr}TABLE ${renderTable(ct.table)}"

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
      val actionSql = action match
        case AddColumn(col) =>
          s"ADD (${renderColumn(col)})"
        case DropColumn(name, cascade) =>
          val cascadeStr = if cascade then " CASCADE CONSTRAINTS" else ""
          s"DROP COLUMN ${quote(name)}$cascadeStr"
        case ModifyColumn(col) =>
          s"MODIFY (${renderColumn(col)})"
        case RenameColumn(oldName, newName) =>
          s"RENAME COLUMN ${quote(oldName)} TO ${quote(newName)}"
        case AddConstraint(constraint) =>
          s"ADD ${summon[SqlRenderer[TableConstraint]].toSql(constraint)}"
        case DropConstraint(name, cascade) =>
          val cascadeStr = if cascade then " CASCADE" else ""
          s"DROP CONSTRAINT ${quote(name)}$cascadeStr"
      s"ALTER TABLE ${quote(at.tableName)} $actionSql"
    }

  def renderCreateIndex(ci: CreateIndex): String = ci.toSql

  def renderDropIndex(di: DropIndex): String = di.toSql

object OracleMigrationExecutor extends MigrationExecutor[CommonDataType, NoColumnOptions.type, NoTableOptions.type]:
  val renderer: SchemaDiffRenderer[CommonDataType, NoColumnOptions.type, NoTableOptions.type] =
    OracleSchemaDiffRenderer

  val supportsTransactionalDDL: Boolean = false

object OracleFullSchemaDiffRenderer extends SchemaDiffRenderer[OracleDataType, OracleColumnOptions, OracleTableOptions]:
  def renderCreateTable(ct: CreateTable[OracleDataType, OracleColumnOptions, OracleTableOptions]): String =
    ct.asInstanceOf[CreateOracleTable].toSql

  def renderDropTable(dt: DropTable): String = dt.toSql

  def renderAlterTable(
    at: AlterTable[
      OracleDataType,
      OracleColumnOptions,
      OracleTableOptions,
      AlterTableAction[OracleDataType, OracleColumnOptions]
    ]
  ): Seq[String] =
    at.actions.map { action =>
      val singleActionAt =
        AlterTable[OracleDataType, OracleColumnOptions, OracleTableOptions, OracleAlterTableAction](
          at.tableName,
          Seq(action.asInstanceOf[OracleAlterTableAction])
        )
      singleActionAt.toSql
    }

  def renderCreateIndex(ci: CreateIndex): String = ci.toSql

  def renderDropIndex(di: DropIndex): String = di.toSql

object OracleFullMigrationExecutor extends MigrationExecutor[OracleDataType, OracleColumnOptions, OracleTableOptions]:
  val renderer: SchemaDiffRenderer[OracleDataType, OracleColumnOptions, OracleTableOptions] =
    OracleFullSchemaDiffRenderer

  val supportsTransactionalDDL: Boolean = false
