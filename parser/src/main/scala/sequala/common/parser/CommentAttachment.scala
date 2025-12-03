package sequala.common.parser

import fastparse.*
import fastparse.internal.{Instrument, Msgs, Util}
import scala.collection.mutable

import sequala.schema.ast.{BlockComment, LineComment, SqlComment}

/** Character-position-based comment, used during parsing. */
case class CharPositionedComment(comment: SqlComment, startPos: Int, endPos: Int)

/** Mutable context for collecting comments during parsing and associating them with parsed elements. */
class ParseContext {
  val collectedComments: mutable.ArrayBuffer[CharPositionedComment] = mutable.ArrayBuffer.empty
  val pendingAttachments: mutable.Map[Int, Seq[SqlComment]] = mutable.Map.empty

  def addComment(comment: SqlComment, startPos: Int, endPos: Int): Unit =
    collectedComments += CharPositionedComment(comment, startPos, endPos)

  def associateCommentsAt(parserStartPos: Int): Unit = {
    val preceding = collectedComments
      .filter { c =>
        c.endPos <= parserStartPos && parserStartPos - c.endPos <= 50
      }
      .sortBy(_.endPos)

    if preceding.nonEmpty then {
      var lastEnd = -1
      val contiguous = preceding.takeWhile { c =>
        val ok = lastEnd < 0 || c.startPos - lastEnd <= 2
        lastEnd = c.endPos
        ok
      }

      if contiguous.nonEmpty then {
        pendingAttachments(parserStartPos) = contiguous.map(_.comment).toSeq
        collectedComments --= contiguous
      }
    }
  }

  def getCommentsFor(startPos: Int): Seq[SqlComment] =
    pendingAttachments.getOrElse(startPos, Seq.empty)

  def clear(): Unit = {
    collectedComments.clear()
    pendingAttachments.clear()
  }
}

/** Whitespace parser that collects SQL comments into a ParseContext instead of discarding them. This allows comments to
  * be later attached to parsed elements based on position.
  */
class CommentCollectingWhitespace(ctx: ParseContext) extends Whitespace {
  def apply(pctx: ParsingRun[?]): ParsingRun[Unit] = {
    val input = pctx.input
    @scala.annotation.tailrec
    def rec(current: Int, state: Int, commentStart: Int): ParsingRun[Unit] =
      if !input.isReachable(current) then {
        state match {
          case 0 | 2 =>
            if state == 2 && commentStart >= 0 then {
              val text = input.slice(commentStart + 2, current).toString.trim
              ctx.addComment(LineComment(text), commentStart, current)
            }
            if pctx.verboseFailures then pctx.reportTerminalMsg(current, Msgs.empty)
            pctx.freshSuccessUnit(current)
          case 1 | 3 =>
            if pctx.verboseFailures then pctx.reportTerminalMsg(current - 1, Msgs.empty)
            pctx.freshSuccessUnit(current - 1)
          case _ =>
            pctx.cut = true
            val res = pctx.freshFailure(current)
            if pctx.verboseFailures then pctx.reportTerminalMsg(current, () => Util.literalize("*/"))
            res
        }
      } else {
        val currentChar = input(current)
        (state: @scala.annotation.switch) match {
          case 0 =>
            (currentChar: @scala.annotation.switch) match {
              case ' ' | '\t' | '\n' | '\r' => rec(current + 1, state, -1)
              case '-' => rec(current + 1, state = 1, current)
              case '/' => rec(current + 1, state = 3, current)
              case _ =>
                if pctx.verboseFailures then pctx.reportTerminalMsg(current, Msgs.empty)
                pctx.freshSuccessUnit(current)
            }
          case 1 =>
            if currentChar == '-' then {
              rec(current + 1, state = 2, commentStart)
            } else {
              if pctx.verboseFailures then pctx.reportTerminalMsg(current - 1, Msgs.empty)
              pctx.freshSuccessUnit(current - 1)
            }
          case 2 =>
            if currentChar == '\n' then {
              val text = input.slice(commentStart + 2, current).toString.trim
              ctx.addComment(LineComment(text), commentStart, current)
              rec(current + 1, state = 0, -1)
            } else {
              rec(current + 1, state, commentStart)
            }
          case 3 =>
            (currentChar: @scala.annotation.switch) match {
              case '*' => rec(current + 1, state = 4, commentStart)
              case _ =>
                if pctx.verboseFailures then pctx.reportTerminalMsg(current - 1, Msgs.empty)
                pctx.freshSuccessUnit(current - 1)
            }
          case 4 =>
            rec(current + 1, state = if currentChar == '*' then 5 else state, commentStart)
          case 5 =>
            (currentChar: @scala.annotation.switch) match {
              case '/' =>
                val text = input.slice(commentStart + 2, current - 1).toString.trim
                ctx.addComment(BlockComment(text), commentStart, current + 1)
                rec(current + 1, state = 0, -1)
              case '*' => rec(current + 1, state = 5, commentStart)
              case _ => rec(current + 1, state = 4, commentStart)
            }
        }
      }
    rec(current = pctx.index, state = 0, commentStart = -1)
  }
}

/** FastParse Instrument that associates collected comments with parser positions. */
class CommentAttachingInstrument(ctx: ParseContext, targetParsers: Set[String] = Set.empty) extends Instrument {
  private val parserStack: mutable.Stack[(String, Int)] = mutable.Stack.empty

  def beforeParse(parser: String, index: Int): Unit = {
    parserStack.push((parser, index))
    if targetParsers.isEmpty || targetParsers.contains(parser) then {
      ctx.associateCommentsAt(index)
    }
  }

  def afterParse(parser: String, index: Int, success: Boolean): Unit = {
    parserStack.pop()
  }
}

/** Typeclass for types that can have source comments attached. */
trait HasSourceComment[A] {
  def attachComments(a: A, comments: Seq[SqlComment]): A
}

object HasSourceComment {
  given columnHasSourceComment[DT, CO <: sequala.schema.ColumnOptions]: HasSourceComment[sequala.schema.Column[DT, CO]]
  with {
    def attachComments(col: sequala.schema.Column[DT, CO], comments: Seq[SqlComment]): sequala.schema.Column[DT, CO] =
      col.copy(sourceComment = comments)
  }

  given tableHasSourceComment[DT, CO <: sequala.schema.ColumnOptions, TO <: sequala.schema.TableOptions]
    : HasSourceComment[sequala.schema.Table[DT, CO, TO]] with {
    def attachComments(
      table: sequala.schema.Table[DT, CO, TO],
      comments: Seq[SqlComment]
    ): sequala.schema.Table[DT, CO, TO] =
      table.copy(sourceComment = comments)
  }

  given noOpHasSourceComment[A]: HasSourceComment[A] with {
    def attachComments(a: A, comments: Seq[SqlComment]): A = a
  }
}

/** Parser extensions for comment attachment. */
object CommentAttachmentSyntax {
  import fastparse.NoWhitespace.noWhitespaceImplicit

  extension [A](p: P[A])
    /** Attach any comments that were associated with this parser's start position. Requires a ParseContext and
      * HasSourceComment instance.
      */
    def withComments(using ctx: ParseContext, attach: HasSourceComment[A], ev: P[Any]): P[A] =
      (Index ~ p).map { (startPos, a) =>
        val comments = ctx.getCommentsFor(startPos)
        if comments.nonEmpty then attach.attachComments(a, comments) else a
      }
}
