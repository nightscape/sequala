package sequala.common.parser

import fastparse.*
import scala.io.*
import java.io.*
import sequala.common.statement.*
import sequala.schema.statement.*
import sequala.schema.Statement
import sequala.ansi.ANSISQL

/** Backward-compatible SQL parser object. Delegates to ANSISQL for parsing.
  *
  * @deprecated
  *   Consider using ANSISQL, OracleSQL, or PostgresSQL directly for dialect-specific parsing
  */
object SQL {
  def apply(input: String): Parsed[Statement] = ANSISQL(input)
  def apply(input: Reader): StreamParser[Statement] = ANSISQL(input)
}
