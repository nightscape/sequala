package sequala.migrate

import sequala.schema.*

case class MigrationStep[DT <: DataType, CO <: ColumnOptions, TO <: TableOptions](
  statement: SchemaDiff[DT, CO, TO],
  reverse: Option[SchemaDiff[DT, CO, TO]],
  comment: String
)
