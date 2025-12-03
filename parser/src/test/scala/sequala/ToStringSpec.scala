package sequala

import org.specs2.mutable.*
import sequala.common.parser.SQL
import sequala.common.renderer.ParserSqlRenderers.given
import sequala.schema.{SqlFormatConfig, SqlRenderer}
import sequala.schema.SqlRenderer.toSql
import fastparse.Parsed

import scala.io.*
import java.io.*

class ToStringSpec extends Specification {
  given SqlFormatConfig = SqlFormatConfig.Compact

  def testQuery(q: String) =
    SQL(q + ";") match {
      case Parsed.Success(parsed, _) => parsed.toSql must beEqualTo(q + ";")
      case f: Parsed.Failure => f.trace().longMsg must beEqualTo("")
    }

  "SQL should reversibly translate" >> {
    "SELECT 1" >> testQuery("SELECT 1")
    "SELECT A FROM R" >> testQuery("SELECT A FROM R")
    "SELECT A FROM (SELECT A FROM R) AS X" >> testQuery("SELECT A FROM (SELECT A FROM R) AS X")
    "SELECT A FROM (SELECT A FROM R) AS X, Q" >> testQuery("SELECT A FROM (SELECT A FROM R) AS X, Q")
    "SELECT 1 WHERE true" >> testQuery("SELECT 1 WHERE true")
    "SELECT 'Smith' FROM R WHERE true" >> testQuery("SELECT 'Smith' FROM R WHERE true")
    "SELECT 332.1" >> testQuery("SELECT 332.1")
    "SELECT A, SUM(B) FROM R GROUP BY A" >> testQuery("SELECT A, SUM(B) FROM R GROUP BY A")
    "SELECT A, SUM(B) FROM R GROUP BY A HAVING COUNT(*) > 2" >> testQuery(
      "SELECT A, SUM(B) FROM R GROUP BY A HAVING COUNT(*) > 2"
    )
    "SELECT A, SUM(B) FROM R GROUP BY A HAVING (COUNT(*) > 2) AND (SUM(C) < 9)" >> testQuery(
      "SELECT A, SUM(B) FROM R GROUP BY A HAVING (COUNT(*) > 2) AND (SUM(C) < 9)"
    )
    "SELECT 1 UNION DISTINCT SELECT 2" >> testQuery("SELECT 1 UNION DISTINCT SELECT 2")
    "SELECT 1 UNION ALL SELECT 2" >> testQuery("SELECT 1 UNION ALL SELECT 2")
    "SELECT A FROM R ORDER BY B, C, D DESC" >> testQuery("SELECT A FROM R ORDER BY B, C, D DESC")
    "SELECT A FROM R ORDER BY B, C LIMIT 2" >> testQuery("SELECT A FROM R ORDER BY B, C LIMIT 2")
    "SELECT A FROM R ORDER BY B, C LIMIT 2 OFFSET 3" >> testQuery("SELECT A FROM R ORDER BY B, C LIMIT 2 OFFSET 3")
    "SELECT A FROM R ORDER BY B, C OFFSET 3" >> testQuery("SELECT A FROM R ORDER BY B, C OFFSET 3")
    "INSERT INTO R(A, B) VALUES (1, 2), (3, 4)" >> testQuery("INSERT INTO R(A, B) VALUES (1, 2), (3, 4)")
    "DELETE FROM R WHERE x = 2" >> testQuery("DELETE FROM R WHERE x = 2")
    "DELETE FROM R" >> testQuery("DELETE FROM R")
    "UPDATE R SET A = 23 + foo(bar, 9.2), B = 9 WHERE false" >> testQuery(
      "UPDATE R SET A = 23 + foo(bar, 9.2), B = 9 WHERE false"
    )
    "EXPLAIN SELECT 1" >> testQuery("EXPLAIN SELECT 1")
    "CREATE TABLE R(A int, B float, C varchar(19))" >> {
      SQL("CREATE TABLE R(A int, B float, C varchar(19));") match {
        case Parsed.Success(parsed, _) =>
          parsed.toSql must contain("CREATE TABLE")
          parsed.toSql must contain("R")
          parsed.toSql must contain("INTEGER")
          parsed.toSql must contain("REAL")
          parsed.toSql must contain("VARCHAR(19)")
        case f: Parsed.Failure => f.trace().longMsg must beEqualTo("")
      }
    }
    "CREATE OR REPLACE TABLE R(A int PRIMARY KEY NOT NULL, B float, C varchar(19))" >> {
      SQL("CREATE OR REPLACE TABLE R(A int PRIMARY KEY NOT NULL, B float, C varchar(19));") match {
        case Parsed.Success(parsed, _) =>
          parsed.toSql must contain("CREATE OR REPLACE TABLE")
          parsed.toSql must contain("R")
          parsed.toSql must contain("NOT NULL")
          parsed.toSql must contain("PRIMARY KEY")
        case f: Parsed.Failure => f.trace().longMsg must beEqualTo("")
      }
    }
    "INSERT INTO R SELECT 1" >> testQuery("INSERT INTO R SELECT 1")
  }
}
