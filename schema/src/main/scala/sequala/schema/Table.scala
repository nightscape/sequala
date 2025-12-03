package sequala.schema

import sequala.schema.ast.SqlComment

case class Table[DT, CO <: ColumnOptions, TO <: TableOptions](
  name: String,
  columns: Seq[Column[DT, CO]],
  primaryKey: Option[PrimaryKey] = None,
  indexes: Seq[Index] = Seq.empty,
  foreignKeys: Seq[ForeignKey] = Seq.empty,
  checks: Seq[Check] = Seq.empty,
  uniques: Seq[Unique] = Seq.empty,
  options: TO,
  comment: Option[String] = None,
  schema: Option[String] = None,
  sourceComment: Seq[SqlComment] = Seq.empty
) {

  /** Fully qualified name: schema.name if schema is set, otherwise just name */
  def qualifiedName: String = schema.map(s => s"$s.$name").getOrElse(name)
}

object Table {
  def apply[DT, CO <: ColumnOptions](name: String, columns: Seq[Column[DT, CO]]): Table[DT, CO, NoTableOptions.type] =
    Table(name, columns, None, Seq.empty, Seq.empty, Seq.empty, Seq.empty, NoTableOptions)

  def apply[DT, CO <: ColumnOptions](
    name: String,
    columns: Seq[Column[DT, CO]],
    primaryKey: Option[PrimaryKey]
  ): Table[DT, CO, NoTableOptions.type] =
    Table(name, columns, primaryKey, Seq.empty, Seq.empty, Seq.empty, Seq.empty, NoTableOptions)
}
