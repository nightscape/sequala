package sequala.schema

case class PrimaryKey(name: Option[String] = None, columns: Seq[String])

case class ForeignKey(
  name: Option[String] = None,
  columns: Seq[String],
  refTable: String,
  refColumns: Seq[String],
  onUpdate: ReferentialAction = NoAction,
  onDelete: ReferentialAction = NoAction
)

case class Index(
  name: Option[String] = None,
  columns: Seq[IndexColumn],
  unique: Boolean = false,
  where: Option[String] = None
)

case class IndexColumn(name: String, descending: Boolean = false, nullsFirst: Option[Boolean] = None)

object IndexColumn {
  def apply(name: String): IndexColumn = IndexColumn(name, descending = false, nullsFirst = None)
}

case class Check(name: Option[String] = None, expression: String)

case class Unique(name: Option[String] = None, columns: Seq[String])

trait ReferentialAction {
  def toSql: String
}
case object NoAction extends ReferentialAction {
  def toSql: String = "NO ACTION"
}
case object Restrict extends ReferentialAction {
  def toSql: String = "RESTRICT"
}
case object Cascade extends ReferentialAction {
  def toSql: String = "CASCADE"
}
case object SetNull extends ReferentialAction {
  def toSql: String = "SET NULL"
}
case object SetDefault extends ReferentialAction {
  def toSql: String = "SET DEFAULT"
}
