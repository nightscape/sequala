package sequala.schema

import sequala.schema.ast.SqlComment

case class Column[DT, CO <: ColumnOptions](
  name: String,
  dataType: DT,
  nullable: Boolean = true,
  default: Option[String] = None,
  options: CO,
  comment: Option[String] = None,
  sourceComment: Seq[SqlComment] = Seq.empty
)

object Column {
  def apply[DT](name: String, dataType: DT): Column[DT, NoColumnOptions.type] =
    Column(name, dataType, nullable = true, default = None, options = NoColumnOptions)

  def apply[DT](name: String, dataType: DT, nullable: Boolean): Column[DT, NoColumnOptions.type] =
    Column(name, dataType, nullable, default = None, options = NoColumnOptions)

  def apply[DT](
    name: String,
    dataType: DT,
    nullable: Boolean,
    default: Option[String]
  ): Column[DT, NoColumnOptions.type] =
    Column(name, dataType, nullable, default, options = NoColumnOptions)
}
