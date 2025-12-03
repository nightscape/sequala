package sequala.migrate.postgres

import sequala.migrate.*
import sequala.schema.*
import sequala.schema.postgres.*
import sequala.schema.postgres.PostgresSqlRenderer.given
import sequala.schema.SqlRenderer.{toSql, given}

object PostgresSchemaDiffRenderer
    extends SchemaDiffRenderer[PostgresDataType, PostgresColumnOptions, PostgresTableOptions]:
  def renderCreateTable(ct: CreateTable[PostgresDataType, PostgresColumnOptions, PostgresTableOptions]): String =
    ct.asInstanceOf[CreatePostgresTable].toSql

  def renderDropTable(dt: DropTable[?]): String =
    dt.asInstanceOf[DropTable[CommonDropOptions]].toSql

  def renderAlterTable(
    at: AlterTable[
      PostgresDataType,
      PostgresColumnOptions,
      PostgresTableOptions,
      AlterTableAction[PostgresDataType, PostgresColumnOptions]
    ]
  ): Seq[String] =
    at.actions.map { action =>
      val singleActionAt: AlterPostgresTable =
        AlterTable[PostgresDataType, PostgresColumnOptions, PostgresTableOptions, PostgresAlterTableAction](
          at.tableName,
          Seq(action.asInstanceOf[PostgresAlterTableAction])
        )
      summon[SqlRenderer[AlterPostgresTable]].toSql(singleActionAt)
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

object PostgresMigrationExecutor
    extends MigrationExecutor[PostgresDataType, PostgresColumnOptions, PostgresTableOptions]:
  val renderer: SchemaDiffRenderer[PostgresDataType, PostgresColumnOptions, PostgresTableOptions] =
    PostgresSchemaDiffRenderer

  val supportsTransactionalDDL: Boolean = true
