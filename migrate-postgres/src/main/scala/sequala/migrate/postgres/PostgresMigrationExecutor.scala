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

  def renderDropTable(dt: DropTable): String = dt.toSql

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

  def renderCreateIndex(ci: CreateIndex): String = ci.toSql

  def renderDropIndex(di: DropIndex): String = di.toSql

object PostgresMigrationExecutor
    extends MigrationExecutor[PostgresDataType, PostgresColumnOptions, PostgresTableOptions]:
  val renderer: SchemaDiffRenderer[PostgresDataType, PostgresColumnOptions, PostgresTableOptions] =
    PostgresSchemaDiffRenderer

  val supportsTransactionalDDL: Boolean = true
