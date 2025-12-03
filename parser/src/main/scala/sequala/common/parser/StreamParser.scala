package sequala.common.parser

import scala.collection.mutable.Buffer
import scala.io.*
import java.io.*
import fastparse.*
import scribe.Logger

class StreamParser[R](parser: (Iterator[String] => Parsed[R]), source: Reader) extends Iterator[Parsed[R]] {
  private val logger = Logger("sequala.common.parser.StreamParser")
  val bufferedSource = source match {
    case b: BufferedReader => b
    case _ => new BufferedReader(source)
  }
  val buffer = Buffer[String]()

  def load(): Unit =
    while bufferedSource.ready do buffer += bufferedSource.readLine.replace("\\n", " ");

  def loadBlocking(): Unit =
    buffer += bufferedSource.readLine.replace("\\n", " ");

  def hasNext: Boolean = { load(); buffer.size > 0 }
  def next(): Parsed[R] = {
    load();
    if buffer.size > 0 then {
      parser(buffer.iterator) match {
        case r @ Parsed.Success(result, index) =>
          logger.debug(s"Parsed(index = $index): $result")
          skipBytes(index)
          return r
        case f: Parsed.Failure =>
          buffer.clear()
          return f
      }
    } else {
      throw new IndexOutOfBoundsException("reading from an empty iterator")
    }
  }

  def skipBytes(offset: Int): Unit = {
    var dropped = 0
    while offset > dropped && !buffer.isEmpty do {
      logger.trace(s"Checking for drop: $dropped / $offset: Next unit: ${buffer.head.length}")
      if buffer.head.length < (offset - dropped) then {
        dropped = dropped + buffer.head.length
        logger.trace(s"Dropping '${buffer.head}' ($dropped / $offset dropped so far)")
        buffer.remove(0)
      } else {
        logger.trace(s"Trimming '${buffer.head}' (by ${offset - dropped})")
        var head = buffer.head
          .substring(offset - dropped)
          .replace("^\\w+", "")
        logger.trace(s"Trimming leading whitespace")
        while head.length <= 0 && !buffer.isEmpty do {
          logger.trace(s"Nothing but whitespace left.  Dropping and trying again")
          buffer.remove(0)
          if !buffer.isEmpty then {
            head = buffer.head.replace("^\\w+", "")
          }
        }
        logger.trace(s"Remaining = '$head'")
        if head.length > 0 then { buffer.update(0, head) }
        return
      }
    }
  }

}
