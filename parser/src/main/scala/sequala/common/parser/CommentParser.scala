package sequala.common.parser

import fastparse.*
import fastparse.NoWhitespace.*
import sequala.schema.ast.{BlockComment, LineComment, SqlComment}

/** Parsers for SQL comments. These use NoWhitespace to avoid consuming whitespace that might contain comments we want
  * to capture.
  */
object CommentParser {

  /** Parse a line comment: -- ... until end of line */
  def lineComment[$ : P]: P[LineComment] =
    P("--" ~~ CharsWhile(c => c != '\n' && c != '\r', 0).!).map(text => LineComment(text.trim))

  /** Parse a block comment: /* ... */ */
  def blockComment[$ : P]: P[BlockComment] =
    P("/*" ~~ (!"*/" ~~ AnyChar).rep.! ~~ "*/").map(text => BlockComment(text.trim))

  /** Parse any SQL comment */
  def sqlComment[$ : P]: P[SqlComment] = P(lineComment | blockComment)

  /** Parse zero or more comments, consuming whitespace between them. Uses PureWhitespace to not accidentally consume
    * comments as whitespace.
    */
  def comments[$ : P](using ws: Whitespace): P[Seq[SqlComment]] = {
    given Whitespace = PureWhitespace.whitespace
    P(sqlComment.rep)
  }
}
