package sequala.migrate

import sequala.schema.*

case class MigrationStep[DT <: DataType, CO <: ColumnOptions, TO <: TableOptions](
  statement: SchemaDiffOp,
  reverse: Option[SchemaDiffOp],
  comment: String
)
