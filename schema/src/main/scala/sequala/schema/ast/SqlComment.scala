package sequala.schema.ast

sealed trait SqlComment:
  def text: String

case class LineComment(text: String) extends SqlComment
case class BlockComment(text: String) extends SqlComment
