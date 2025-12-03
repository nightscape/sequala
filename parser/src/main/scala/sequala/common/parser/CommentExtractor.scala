package sequala.common.parser

import sequala.schema.ast.{BlockComment, LineComment, SqlComment}

/** Extracts SQL comments from source text with their line numbers. Used for post-parsing comment attachment.
  */
object CommentExtractor {

  case class PositionedComment(comment: SqlComment, line: Int, endLine: Int)

  /** Convert a character position to a line number (1-based). */
  def positionToLine(input: String, pos: Int): Int = {
    var line = 1
    var i = 0
    while i < pos && i < input.length do {
      if input(i) == '\n' then line += 1
      i += 1
    }
    line
  }

  /** Extract all comments from the input with their line positions.
    * @param input
    *   SQL source text
    * @return
    *   Sequence of comments with their starting and ending line numbers (1-based)
    */
  def extract(input: String): Seq[PositionedComment] = {
    val result = scala.collection.mutable.ArrayBuffer[PositionedComment]()
    var pos = 0
    var line = 1

    while pos < input.length do {
      val char = input(pos)

      if char == '\n' then {
        line += 1
        pos += 1
      } else if char == '-' && pos + 1 < input.length && input(pos + 1) == '-' then {
        val startLine = line
        val startPos = pos + 2
        pos += 2
        while pos < input.length && input(pos) != '\n' do pos += 1
        val text = input.substring(startPos, pos).trim
        result += PositionedComment(LineComment(text), startLine, startLine)
      } else if char == '/' && pos + 1 < input.length && input(pos + 1) == '*' then {
        val startLine = line
        val startPos = pos + 2
        pos += 2
        while pos + 1 < input.length && !(input(pos) == '*' && input(pos + 1) == '/') do {
          if input(pos) == '\n' then line += 1
          pos += 1
        }
        val endLine = line
        val text = if pos < input.length then input.substring(startPos, pos).trim else ""
        pos += 2
        result += PositionedComment(BlockComment(text), startLine, endLine)
      } else if char == '\'' then {
        pos += 1
        while pos < input.length do {
          if input(pos) == '\'' then {
            if pos + 1 < input.length && input(pos + 1) == '\'' then {
              pos += 2
            } else {
              pos += 1
              pos = input.length + 1 // break inner loop
            }
          } else {
            if input(pos) == '\n' then line += 1
            pos += 1
          }
        }
        if pos == input.length + 1 then pos = input.length
      } else {
        pos += 1
      }
    }

    result.toSeq
  }

  /** Find comments that should be associated with a given line. A comment is associated with a line if:
    *   - It's a line comment on the same line (trailing comment)
    *   - It's a line/block comment on immediately preceding lines (leading comment)
    *
    * @param comments
    *   All extracted comments
    * @param targetLine
    *   The line of the element to find comments for
    * @return
    *   Comments associated with this line
    */
  def commentsForLine(comments: Seq[PositionedComment], targetLine: Int): Seq[SqlComment] = {
    val result = scala.collection.mutable.ArrayBuffer[SqlComment]()

    // Find comments on the same line (trailing) or immediately before (leading)
    var currentLine = targetLine
    val commentsOnOrBefore = comments.filter(c => c.line <= targetLine && c.endLine >= targetLine - 10)

    // Comments on the same line
    commentsOnOrBefore.filter(_.line == targetLine).foreach(c => result += c.comment)

    // Comments on preceding lines (walk backwards until we hit a gap or non-comment content)
    val precedingComments = commentsOnOrBefore
      .filter(c => c.endLine < targetLine)
      .sortBy(-_.endLine)

    var prevEndLine = targetLine
    for pc <- precedingComments do {
      if pc.endLine >= prevEndLine - 1 then {
        result.prepend(pc.comment)
        prevEndLine = pc.line
      }
    }

    result.toSeq
  }
}
