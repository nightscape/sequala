package sequala.ansi

import fastparse._
import scala.io._
import java.io._
import sequala.common.statement._
import sequala.common.parser.{SQLBase, SQLBaseObject, StreamParser}
import sequala.schema.{
  DataType,
  Decimal,
  DoublePrecision,
  Real,
  SqlBigInt,
  SqlBoolean,
  SqlChar,
  SqlDate,
  SqlInteger,
  SqlText,
  SqlTimestamp,
  VarChar
}

/** ANSI SQL dialect implementation. Provides baseline SQL parsing with standard ANSI SQL features.
  */
class ANSISQL extends SQLBase {
  // Configuration
  type Stmt = Statement
  def caseSensitive = false
  def statementTerminator = ";"
  def supportsCTEs = true
  def supportsReturning = false
  def supportsIfExists = true
  def stringConcatOp = "||"

  // Parser implementations
  def createStatement[$ : P]: P[Statement] = P(&(keyword("CREATE")) ~/ (createIndex | createTable | createView))

  def dialectSpecificStatement[$ : P]: P[Statement] = P(fastparse.Fail)

  def dataType[$ : P]: P[DataType] = P(
    (keyword("VARCHAR") ~ "(" ~ integer.map(_.toInt) ~ ")").map(VarChar(_))
      | (keyword("CHAR") ~ "(" ~ integer.map(_.toInt) ~ ")").map(SqlChar(_))
      | keyword("INTEGER").map(_ => SqlInteger)
      | keyword("INT").map(_ => SqlInteger)
      | keyword("BIGINT").map(_ => SqlBigInt)
      | (keyword("DECIMAL") ~ "(" ~ integer.map(_.toInt) ~ "," ~ integer.map(_.toInt) ~ ")")
        .map { case (p, s) => Decimal(p, s) }
      | keyword("FLOAT").map(_ => Real)
      | keyword("DOUBLE").map(_ => DoublePrecision)
      | keyword("DATE").map(_ => SqlDate)
      | (keyword("TIMESTAMP") ~ ("(" ~ integer.map(_.toInt) ~ ")").?)
        .map(p => SqlTimestamp(p))
      | keyword("BOOLEAN").map(_ => SqlBoolean)
      | keyword("TEXT").map(_ => SqlText)
  )
}

object ANSISQL extends SQLBaseObject {
  type Stmt = Statement
  import fastparse.Parsed
  val instance = new ANSISQL()

  def name: String = "ansi"

  protected def statementTerminator: String = ";"

  def apply(input: String): Parsed[Stmt] =
    parse(input, instance.terminatedStatement(_))

  def apply(input: Reader): StreamParser[Stmt] =
    new StreamParser[Stmt](parse(_: Iterator[String], instance.terminatedStatement(_), verboseFailures = true), input)
}
