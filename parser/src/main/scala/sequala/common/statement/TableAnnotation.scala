package sequala.common.statement

import sequala.common.Name
import sequala.common.expression.ToSql
import sequala.schema.ReferentialAction

sealed abstract class TableAnnotation extends ToSql

case class TablePrimaryKey(columns: Seq[Name]) extends TableAnnotation {
  override def toSql = s"PRIMARY KEY (${columns.map(_.toSql).mkString(", ")})"
}
case class TableIndexOn(columns: Seq[Name]) extends TableAnnotation {
  override def toSql = s"INDEX (${columns.map(_.toSql).mkString(", ")})"
}
case class TableUnique(columns: Seq[Name]) extends TableAnnotation {
  override def toSql = s"UNIQUE (${columns.map(_.toSql).mkString(", ")})"
}

case class TableForeignKey(
  name: Option[Name],
  columns: Seq[Name],
  refTable: Name,
  refColumns: Seq[Name],
  onUpdate: Option[ReferentialAction] = None,
  onDelete: Option[ReferentialAction] = None
) extends TableAnnotation {
  override def toSql: String = {
    val constraintName = name.map(n => s"CONSTRAINT ${n.toSql} ").getOrElse("")
    val colList = columns.map(_.toSql).mkString(", ")
    val refColList = refColumns.map(_.toSql).mkString(", ")
    val onUpdateStr = onUpdate.map(a => s" ON UPDATE ${a.toSql}").getOrElse("")
    val onDeleteStr = onDelete.map(a => s" ON DELETE ${a.toSql}").getOrElse("")
    s"${constraintName}FOREIGN KEY ($colList) REFERENCES ${refTable.toSql}($refColList)$onUpdateStr$onDeleteStr"
  }
}
