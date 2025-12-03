package sequala.postgres

import fastparse.*
import scala.io.*
import java.io.*
import sequala.common.statement.*
import sequala.schema.statement.*
import sequala.schema.ast.{FromElement, OrderBy, SelectBody, SelectTarget, Union, *}
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
  Statement,
  VarChar
}
import sequala.schema.postgres.{BigSerial, Cidr, Inet, Json, Jsonb, MacAddr, PgArray, Serial, Uuid}

/** Postgres SQL dialect implementation. Supports Postgres-specific features including SERIAL, JSONB, UUID types, and
  * Postgres CREATE TABLE extensions (INHERITS, etc.).
  */
class PostgresSQL extends SQLBase {
  // Configuration
  type Stmt = Statement // Or PostgresStatement if needed
  def caseSensitive = true // Postgres preserves case in identifiers
  def statementTerminator = ";"
  def supportsCTEs = true
  def supportsReturning = true
  def supportsIfExists = true
  def stringConcatOp = "||"

  // Postgres-specific config
  def supportsInheritance = true
  def supportsOnConflict = true

  // Postgres can mostly use ANSI base implementations
  // but override where needed (e.g., data types, ON CONFLICT)
  def createStatement[$ : P]: P[Statement] = P(&(keyword("CREATE")) ~/ (createIndex | postgresCreateTable | createView))

  def postgresCreateTable[$ : P]: P[Statement] = P(
    // Similar to ANSI but with Postgres extensions:
    // - INHERITS clause (can be added later)
    // - Different storage parameters
    // - UNLOGGED, TEMPORARY options
    createTable // For now, use base implementation
  )

  def dialectSpecificStatement[$ : P]: P[Statement] = P(
    fastparse.Fail // No dialect-specific statements yet
  )

  def dataType[$ : P]: P[DataType] = P(
    // Postgres-specific types first
    keyword("SERIAL").map(_ => Serial)
      | keyword("BIGSERIAL").map(_ => BigSerial)
      | keyword("JSONB").map(_ => Jsonb)
      | keyword("JSON").map(_ => Json)
      | keyword("UUID").map(_ => Uuid)
      | keyword("INET").map(_ => Inet)
      | keyword("CIDR").map(_ => Cidr)
      | keyword("MACADDR").map(_ => MacAddr)
      | (keyword("ARRAY") ~ "[" ~ dataType ~ "]")
        .map(elemType => PgArray(elemType))
      // Fall back to common types
      | (keyword("VARCHAR") ~ "(" ~ integer.map(_.toInt) ~ ")").map(VarChar(_))
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

object PostgresSQL extends SQLBaseObject {
  type Stmt = Statement
  import fastparse.*
  import fastparse.Parsed
  import io.circe.Json
  private val instance = new PostgresSQL()

  def name: String = "postgres"

  protected def statementTerminator: String = ";"

  def apply(input: String): Parsed[Stmt] =
    parse(input, instance.terminatedStatement(_))

  def apply(input: Reader): StreamParser[Stmt] =
    new StreamParser[Stmt](parse(_: Iterator[String], instance.terminatedStatement(_), verboseFailures = true), input)

  def statementToJson(stmt: Statement): Json =
    sequala.schema.postgres.codec.PostgresCirceCodecs.dialectStatementEncoder(stmt)

  def statementToSql(stmt: Statement, pretty: Boolean = false): String = {
    import sequala.schema.{SqlFormatConfig, SqlRenderer}
    import sequala.common.renderer.ParserSqlRenderers.given
    given SqlFormatConfig = if pretty then SqlFormatConfig.Pretty else SqlFormatConfig.Compact
    SqlRenderer[Statement].toSql(stmt)
  }
}
