package sequala.migrate.inspect

import sequala.schema.*
import java.sql.Connection

trait SchemaInspector[DT, CO <: ColumnOptions, TO <: TableOptions]:
  def inspectTables(connection: Connection, schemaName: String): Seq[Table[DT, CO, TO]]
  def inspectTable(connection: Connection, schemaName: String, tableName: String): Option[Table[DT, CO, TO]]
