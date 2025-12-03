package sequala.migrate

import sequala.schema.*
import sequala.schema.GenericSqlRenderer.given
import sequala.schema.SqlRenderer.{toSql, given}

object GenericSchemaDiffRenderer extends SchemaDiffRenderer[CommonDataType, NoColumnOptions.type, NoTableOptions.type]:
  def renderCreateTable(ct: CreateTable[CommonDataType, NoColumnOptions.type, NoTableOptions.type]): String =
    ct.asInstanceOf[CreateGenericTable].toSql

  def renderDropTable(dt: DropTable[?]): String =
    dt.asInstanceOf[DropTable[CommonDropOptions]].toSql

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

  def renderCreateIndex(ci: CreateIndex[?]): String = ci.asInstanceOf[CreateIndex[CommonIndexOptions]].toSql

  def renderDropIndex(di: DropIndex): String = di.toSql

  private def quote(name: String): String = s""""$name""""

  def renderSetTableComment(stc: SetTableComment): String =
    val commentStr = stc.comment.map(c => s"'${c.replace("'", "''")}'").getOrElse("NULL")
    s"COMMENT ON TABLE ${quote(stc.tableName)} IS $commentStr"

  def renderSetColumnComment(scc: SetColumnComment): String =
    val commentStr = scc.comment.map(c => s"'${c.replace("'", "''")}'").getOrElse("NULL")
    s"COMMENT ON COLUMN ${quote(scc.tableName)}.${quote(scc.columnName)} IS $commentStr"

object GenericMigrationExecutor extends MigrationExecutor[CommonDataType, NoColumnOptions.type, NoTableOptions.type]:
  val renderer: SchemaDiffRenderer[CommonDataType, NoColumnOptions.type, NoTableOptions.type] =
    GenericSchemaDiffRenderer

  val supportsTransactionalDDL: Boolean = true
