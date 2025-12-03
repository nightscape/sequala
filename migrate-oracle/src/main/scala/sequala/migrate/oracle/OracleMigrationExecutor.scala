package sequala.migrate.oracle

import sequala.migrate.*
import sequala.schema.*
import sequala.schema.oracle.*
import sequala.schema.oracle.OracleSqlRenderer.given
import sequala.schema.SqlRenderer.{toSql, given}
import sequala.oracle.renderer.OracleParserSqlRenderers.{
  commonActionRenderer,
  commonAlterTableRenderer,
  commonColumnRenderer,
  commonCreateTableRenderer,
  commonTableRenderer
}

/** Renders generic schema operations with Oracle-compatible SQL for data types. Uses SqlRenderer infrastructure. */
object OracleSchemaDiffRenderer extends SchemaDiffRenderer[CommonDataType, NoColumnOptions.type, NoTableOptions.type]:

  private def quote(name: String): String = s""""$name""""
  private def quoteQualifiedName(name: String): String = name.split('.').map(quote).mkString(".")

  def renderCreateTable(ct: CreateTable[CommonDataType, NoColumnOptions.type, NoTableOptions.type]): String =
    ct.toSql

  def renderDropTable(dt: DropTable[?]): String =
    import sequala.schema.oracle.OracleSqlRenderer.given
    import sequala.schema.GenericSqlRenderer.given_SqlRenderer_CommonDropOptions
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
      val singleActionAt = AlterTable[CommonDataType, NoColumnOptions.type, NoTableOptions.type, AlterTableAction[
        CommonDataType,
        NoColumnOptions.type
      ]](at.tableName, Seq(action))
      singleActionAt.toSql
    }

  def renderCreateIndex(ci: CreateIndex[?]): String = ci.asInstanceOf[CreateIndex[CommonIndexOptions]].toSql

  def renderDropIndex(di: DropIndex): String = di.toSql

  def renderSetTableComment(stc: SetTableComment): String =
    val commentStr = stc.comment.map(c => s"'${c.replace("'", "''")}'").getOrElse("''")
    s"COMMENT ON TABLE ${quoteQualifiedName(stc.tableName)} IS $commentStr"

  def renderSetColumnComment(scc: SetColumnComment): String =
    val commentStr = scc.comment.map(c => s"'${c.replace("'", "''")}'").getOrElse("''")
    s"COMMENT ON COLUMN ${quoteQualifiedName(scc.tableName)}.${quote(scc.columnName)} IS $commentStr"

object OracleMigrationExecutor extends MigrationExecutor[CommonDataType, NoColumnOptions.type, NoTableOptions.type]:
  val renderer: SchemaDiffRenderer[CommonDataType, NoColumnOptions.type, NoTableOptions.type] =
    OracleSchemaDiffRenderer

  val supportsTransactionalDDL: Boolean = false

object OracleFullSchemaDiffRenderer extends SchemaDiffRenderer[OracleDataType, OracleColumnOptions, OracleTableOptions]:
  def renderCreateTable(ct: CreateTable[OracleDataType, OracleColumnOptions, OracleTableOptions]): String =
    ct.asInstanceOf[CreateOracleTable].toSql

  def renderDropTable(dt: DropTable[?]): String =
    import sequala.schema.oracle.OracleSqlRenderer.given
    import sequala.schema.GenericSqlRenderer.given_SqlRenderer_CommonDropOptions
    dt.asInstanceOf[DropTable[CommonDropOptions]].toSql

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

  def renderCreateIndex(ci: CreateIndex[?]): String = ci.asInstanceOf[CreateIndex[OracleIndexOptions]].toSql

  def renderDropIndex(di: DropIndex): String = di.toSql

  private def quote(name: String): String = s""""$name""""
  private def quoteQualifiedName(name: String): String = name.split('.').map(quote).mkString(".")

  def renderSetTableComment(stc: SetTableComment): String =
    val commentStr = stc.comment.map(c => s"'${c.replace("'", "''")}'").getOrElse("''")
    s"COMMENT ON TABLE ${quoteQualifiedName(stc.tableName)} IS $commentStr"

  def renderSetColumnComment(scc: SetColumnComment): String =
    val commentStr = scc.comment.map(c => s"'${c.replace("'", "''")}'").getOrElse("''")
    s"COMMENT ON COLUMN ${quoteQualifiedName(scc.tableName)}.${quote(scc.columnName)} IS $commentStr"

object OracleFullMigrationExecutor extends MigrationExecutor[OracleDataType, OracleColumnOptions, OracleTableOptions]:
  val renderer: SchemaDiffRenderer[OracleDataType, OracleColumnOptions, OracleTableOptions] =
    OracleFullSchemaDiffRenderer

  val supportsTransactionalDDL: Boolean = false
