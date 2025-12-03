package sequala.common.statement

import sequala.schema.{
  CreateGenericTable,
  CreateTable as SchemaCreateTable,
  GenericSqlRenderer,
  SqlFormatConfig,
  SqlRenderer
}

case class CreateTableStatement(schemaTable: CreateGenericTable) extends Statement {
  import GenericSqlRenderer.given
  import SqlRenderer.given

  override def toSql: String =
    summon[SqlRenderer[CreateGenericTable]].toSql(schemaTable) + ";"

  def tableName: String = schemaTable.table.name
  def orReplace: Boolean = schemaTable.orReplace
}
