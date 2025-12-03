package sequala

import org.specs2.mutable.*
import sequala.oracle.{OracleSQL, OracleStatement}
import sequala.common.statement.StatementParseResult
import sequala.schema.Unparseable
import sequala.schema.Statement
import fastparse.Parsed
import scala.io.Source

class OracleParserSpec extends Specification {

  "The Oracle SQL Parser" should {
    "preserve named PRIMARY KEY constraint name" in {
      val sql = """CREATE TABLE "TEST_SCHEMA"."TEST_TABLE" (
        "ID" NUMBER NOT NULL,
        "NAME" VARCHAR2(100),
        CONSTRAINT "PK_TEST_TABLE" PRIMARY KEY ("ID")
      )"""

      val result = OracleSQL.parseCreateTableWithComments(sql)
      result must beRight

      val ct = result.toOption.get
      ct.table.primaryKey must beSome
      ct.table.primaryKey.get.columns must_== Seq("ID")
      ct.table.primaryKey.get.name must beSome("PK_TEST_TABLE")
    }

    "preserve named UNIQUE constraint name" in {
      val sql = """CREATE TABLE "TEST_SCHEMA"."TEST_TABLE" (
        "ID" NUMBER NOT NULL,
        "EMAIL" VARCHAR2(100),
        CONSTRAINT "UK_TEST_EMAIL" UNIQUE ("EMAIL")
      )"""

      val result = OracleSQL.parseCreateTableWithComments(sql)
      result must beRight

      val ct = result.toOption.get
      ct.table.uniques must have size 1
      ct.table.uniques.head.columns must_== Seq("EMAIL")
      ct.table.uniques.head.name must beSome("UK_TEST_EMAIL")
    }

    "parse COMMENT ON TABLE statement" in {
      val ddl = """COMMENT ON TABLE "GUI_XMDM"."TEST_TABLE" IS 'This is a test table';"""
      val results = OracleSQL.parseAll(ddl)
      val statements = results.flatMap(_.result.toOption)

      statements must have size 1
      statements.head must beAnInstanceOf[sequala.schema.TableCommentStatement]

      val comment = statements.head.asInstanceOf[sequala.schema.TableCommentStatement]
      comment.tableName must_== "TEST_TABLE"
      comment.comment must_== "This is a test table"
    }

    "parse COMMENT ON COLUMN statement" in {
      val ddl = """COMMENT ON COLUMN "GUI_XMDM"."TEST_TABLE"."ID" IS 'Primary key';"""
      val results = OracleSQL.parseAll(ddl)
      val statements = results.flatMap(_.result.toOption)

      statements must have size 1
      statements.head must beAnInstanceOf[sequala.schema.ColumnCommentStatement]

      val comment = statements.head.asInstanceOf[sequala.schema.ColumnCommentStatement]
      comment.tableName must_== "TEST_TABLE"
      comment.columnName must_== "ID"
      comment.comment must_== "Primary key"
    }

    "extract COMMENT ON TABLE from SchemaBuilder" in {
      val ddl = """
CREATE TABLE "GUI_XMDM"."TEST_TABLE" (
  "ID" NUMBER NOT NULL,
  "NAME" VARCHAR2(100)
);

COMMENT ON TABLE "GUI_XMDM"."TEST_TABLE" IS 'This is a test table';
"""
      val results = OracleSQL.parseAll(ddl)
      val statements = results.flatMap(_.result.toOption)

      statements must have size 2

      import sequala.converter.SchemaBuilder
      val tableComments = SchemaBuilder.extractTableComments(statements)

      tableComments must haveSize(1)
      tableComments.get("TEST_TABLE") must beSome("This is a test table")
    }

    "extract COMMENT ON COLUMN from SchemaBuilder" in {
      val ddl = """
CREATE TABLE "GUI_XMDM"."TEST_TABLE" (
  "ID" NUMBER NOT NULL,
  "NAME" VARCHAR2(100)
);

COMMENT ON COLUMN "GUI_XMDM"."TEST_TABLE"."ID" IS 'Primary key';
COMMENT ON COLUMN "GUI_XMDM"."TEST_TABLE"."NAME" IS 'User name';
"""
      val results = OracleSQL.parseAll(ddl)
      val statements = results.flatMap(_.result.toOption)

      statements must have size 3

      import sequala.converter.SchemaBuilder
      val columnComments = SchemaBuilder.extractColumnComments(statements)

      columnComments must haveSize(2)
      columnComments.get(("TEST_TABLE", "ID")) must beSome("Primary key")
      columnComments.get(("TEST_TABLE", "NAME")) must beSome("User name")
    }

    "parse all statements from oracle.sql" in {
      val content = Source.fromResource("oracle.sql").mkString

      val parseResult = OracleSQL.parseAll(content)
      val expectedFailures = "Some random text that should fail to parse.\n/"

      val failures = parseResult.zipWithIndex
        .collect {
          case (StatementParseResult(Left(Unparseable(content)), _, _), idx) if content != expectedFailures =>
            s"Statement ${idx + 1}: Failed to parse\n`${content}`"
        }
        .mkString("\n")
      failures must beEqualTo("")
    }
  }
}
