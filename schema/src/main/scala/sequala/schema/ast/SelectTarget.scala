package sequala.schema.ast

import sequala.schema.ast.Expression

sealed trait SelectTarget

case class SelectAll() extends SelectTarget

case class SelectTable(table: Name) extends SelectTarget

case class SelectExpression(expression: Expression, alias: Option[Name] = None) extends SelectTarget
