package sequala.migrate

import sequala.schema.Statement
import sequala.schema.ast.{Expression, Name}

sealed trait DataDiffOp extends Statement

case class InsertRow(table: Name, columns: Seq[Name], values: Seq[Expression]) extends DataDiffOp

case class UpdateRow(table: Name, keyColumns: Seq[(Name, Expression)], setColumns: Seq[(Name, Expression)])
    extends DataDiffOp

case class DeleteRow(table: Name, keyColumns: Seq[(Name, Expression)]) extends DataDiffOp

case class DataDiffOptions(keyColumns: Option[Seq[String]] = None, generateDeletes: Boolean = true)
