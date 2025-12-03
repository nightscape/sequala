package sequala.schema.ast

import sequala.schema.ast.Expression

case class OrderBy(expression: Expression, ascending: Boolean = true):
  def descending: Boolean = !ascending
