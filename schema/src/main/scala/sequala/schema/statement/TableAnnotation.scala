package sequala.schema.statement

import sequala.schema.ast.Name
import sequala.schema.ReferentialAction

sealed abstract class TableAnnotation

case class TablePrimaryKey(columns: Seq[Name], name: Option[Name] = None) extends TableAnnotation
case class TableIndexOn(columns: Seq[Name]) extends TableAnnotation
case class TableUnique(columns: Seq[Name], name: Option[Name] = None) extends TableAnnotation

case class TableForeignKey(
  name: Option[Name],
  columns: Seq[Name],
  refTable: Name,
  refColumns: Seq[Name],
  onUpdate: Option[ReferentialAction] = None,
  onDelete: Option[ReferentialAction] = None
) extends TableAnnotation
