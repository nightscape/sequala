package sequala.schema

import sequala.schema.ast.{BlockComment, LineComment, SqlComment}

case class SqlFormatConfig(pretty: Boolean = false, indent: String = "  "):
  def join(items: Seq[String], sep: String = ","): String =
    if pretty then items.mkString(s"$sep\n$indent")
    else items.mkString(s"$sep ")

  def wrap(prefix: String, content: String, suffix: String): String =
    if pretty then s"$prefix\n$indent$content\n$suffix"
    else s"$prefix$content$suffix"

  def renderComment(c: SqlComment): String = c match
    case LineComment(text) => s"-- $text"
    case BlockComment(text) => s"/* $text */"

  def renderLeadingComments(comments: Seq[SqlComment]): String =
    if comments.isEmpty then ""
    else if pretty then comments.map(c => renderComment(c)).mkString("", s"\n$indent", "\n" + indent)
    else comments.map(c => renderComment(c)).mkString("", "\n", "\n")

  def renderTrailingComments(comments: Seq[SqlComment]): String =
    if comments.isEmpty then ""
    else if pretty then ""
    else comments.map(c => s"/* ${c.text} */").mkString(" ", " ", "")

object SqlFormatConfig:
  val Compact: SqlFormatConfig = SqlFormatConfig()
  val Pretty: SqlFormatConfig = SqlFormatConfig(pretty = true)

trait SqlRenderer[A]:
  def toSql(a: A)(using config: SqlFormatConfig): String

object SqlRenderer:
  def apply[A](using renderer: SqlRenderer[A]): SqlRenderer[A] = renderer

  given SqlFormatConfig = SqlFormatConfig.Compact

  extension [A](a: A)(using renderer: SqlRenderer[A], config: SqlFormatConfig) def toSql: String = renderer.toSql(a)

trait IdentifierQuoting:
  def quoteIdentifier(name: String): String

object IdentifierQuoting:
  val DoubleQuote: IdentifierQuoting = new IdentifierQuoting:
    def quoteIdentifier(name: String): String =
      if name.exists(c => c.isWhitespace || c == '"' || !c.isLetterOrDigit && c != '_') then
        s""""${name.replace("\"", "\"\"")}""""
      else name
