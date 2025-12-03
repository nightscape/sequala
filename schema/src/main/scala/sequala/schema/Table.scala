package sequala.schema

case class Table[DT, CO <: ColumnOptions, TO <: TableOptions](
  name: String,
  columns: Seq[Column[DT, CO]],
  primaryKey: Option[PrimaryKey] = None,
  indexes: Seq[Index] = Seq.empty,
  foreignKeys: Seq[ForeignKey] = Seq.empty,
  checks: Seq[Check] = Seq.empty,
  uniques: Seq[Unique] = Seq.empty,
  options: TO
)

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
