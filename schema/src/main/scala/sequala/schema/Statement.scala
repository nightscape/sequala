package sequala.schema

case class CreateTable[DT, CO <: ColumnOptions, TO <: TableOptions](
  table: Table[DT, CO, TO],
  orReplace: Boolean = false,
  ifNotExists: Boolean = false
)

case class DropTable(tableName: String, ifExists: Boolean = false, cascade: Boolean = false)

case class AlterTable[DT, CO <: ColumnOptions, TO <: TableOptions, ATA <: AlterTableAction[DT, CO]](
  tableName: String,
  actions: Seq[ATA]
)

trait AlterTableAction[DT, CO <: ColumnOptions]:
  def toClause: String
  protected def quote(name: String): String = IdentifierQuoting.DoubleQuote.quoteIdentifier(name)

case class AddColumn[DT <: DataType, CO <: ColumnOptions](column: Column[DT, CO]) extends AlterTableAction[DT, CO]:
  def toClause: String =
    val nullStr = if column.nullable then "" else " NOT NULL"
    val defaultStr = column.default.map(d => s" DEFAULT $d").getOrElse("")
    s"ADD COLUMN ${quote(column.name)} ${column.dataType.toSql}$defaultStr$nullStr"

case class DropColumn[DT, CO <: ColumnOptions](columnName: String, cascade: Boolean = false)
    extends AlterTableAction[DT, CO]:
  def toClause: String =
    val cascadeStr = if cascade then " CASCADE" else ""
    s"DROP COLUMN ${quote(columnName)}$cascadeStr"

case class ModifyColumn[DT <: DataType, CO <: ColumnOptions](column: Column[DT, CO]) extends AlterTableAction[DT, CO]:
  def toClause: String = s"ALTER COLUMN ${quote(column.name)} TYPE ${column.dataType.toSql}"

case class RenameColumn[DT, CO <: ColumnOptions](oldName: String, newName: String) extends AlterTableAction[DT, CO]:
  def toClause: String = s"RENAME COLUMN ${quote(oldName)} TO ${quote(newName)}"

case class AddConstraint[DT, CO <: ColumnOptions](constraint: TableConstraint) extends AlterTableAction[DT, CO]:
  def toClause: String = s"ADD ${constraint.toClause}"

case class DropConstraint[DT, CO <: ColumnOptions](constraintName: String, cascade: Boolean = false)
    extends AlterTableAction[DT, CO]:
  def toClause: String =
    val cascadeStr = if cascade then " CASCADE" else ""
    s"DROP CONSTRAINT ${quote(constraintName)}$cascadeStr"

sealed trait TableConstraint:
  def toClause: String
  protected def quote(name: String): String = IdentifierQuoting.DoubleQuote.quoteIdentifier(name)

case class PrimaryKeyConstraint(pk: PrimaryKey) extends TableConstraint:
  def toClause: String =
    val nameStr = pk.name.map(n => s"CONSTRAINT ${quote(n)} ").getOrElse("")
    s"${nameStr}PRIMARY KEY (${pk.columns.map(quote).mkString(", ")})"

case class ForeignKeyConstraint(fk: ForeignKey) extends TableConstraint:
  def toClause: String =
    val nameStr = fk.name.map(n => s"CONSTRAINT ${quote(n)} ").getOrElse("")
    val onUpdateStr = fk.onUpdate match
      case NoAction => ""
      case action => s" ON UPDATE ${action.toSql}"
    val onDeleteStr = fk.onDelete match
      case NoAction => ""
      case action => s" ON DELETE ${action.toSql}"
    s"${nameStr}FOREIGN KEY (${fk.columns.map(quote).mkString(", ")}) REFERENCES ${quote(fk.refTable)}(${fk.refColumns.map(quote).mkString(", ")})$onUpdateStr$onDeleteStr"

case class UniqueConstraint(unique: Unique) extends TableConstraint:
  def toClause: String =
    val nameStr = unique.name.map(n => s"CONSTRAINT ${quote(n)} ").getOrElse("")
    s"${nameStr}UNIQUE (${unique.columns.map(quote).mkString(", ")})"

case class CheckConstraint(check: Check) extends TableConstraint:
  def toClause: String =
    val nameStr = check.name.map(n => s"CONSTRAINT ${quote(n)} ").getOrElse("")
    s"${nameStr}CHECK (${check.expression})"

case class CreateIndex(
  name: String,
  tableName: String,
  columns: Seq[IndexColumn],
  unique: Boolean = false,
  ifNotExists: Boolean = false,
  where: Option[String] = None
)

case class DropIndex(name: String, ifExists: Boolean = false, cascade: Boolean = false)
