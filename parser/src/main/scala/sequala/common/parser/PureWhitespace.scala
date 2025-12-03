package sequala.common.parser

import fastparse.*
import fastparse.ParsingRun
import fastparse.internal.Msgs

/** Whitespace parser that only handles spaces, tabs, and newlines. Does NOT consume SQL comments (-- or /* */),
  * allowing them to be parsed explicitly.
  */
object PureWhitespace {
  implicit object whitespace extends Whitespace {
    def apply(ctx: ParsingRun[?]): ParsingRun[Unit] = {
      val input = ctx.input
      @scala.annotation.tailrec
      def rec(current: Int): ParsingRun[Unit] =
        if !input.isReachable(current) then {
          if ctx.verboseFailures then ctx.reportTerminalMsg(current, Msgs.empty)
          ctx.freshSuccessUnit(current)
        } else {
          val currentChar = input(current)
          currentChar match {
            case ' ' | '\t' | '\n' | '\r' => rec(current + 1)
            case _ =>
              if ctx.verboseFailures then ctx.reportTerminalMsg(current, Msgs.empty)
              ctx.freshSuccessUnit(current)
          }
        }
      rec(current = ctx.index)
    }
  }
}
