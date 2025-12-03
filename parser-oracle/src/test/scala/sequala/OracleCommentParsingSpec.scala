package sequala

import org.specs2.mutable.*
import sequala.oracle.OracleSQL
import sequala.schema.{Column, CreateTable, SqlFormatConfig, SqlRenderer}
import sequala.schema.SqlRenderer.toSql
import sequala.schema.ast.{BlockComment, LineComment, SqlComment}
import sequala.schema.oracle.{CreateOracleTable, OracleSqlRenderer}
import sequala.schema.oracle.OracleSqlRenderer.given
import io.circe.syntax.*

class OracleCommentParsingSpec extends Specification {

  "OracleSQL.parseCreateTableWithComments" should {
    "parse table-level comments" in {
      val sql = """-- This is the users table
                  |CREATE TABLE users (
                  |  id NUMBER PRIMARY KEY
                  |)""".stripMargin

      val result = OracleSQL.parseCreateTableWithComments(sql)

      result must beRight
      val ct = result.toOption.get
      ct.table.sourceComment must have size 1
      ct.table.sourceComment.head must beAnInstanceOf[LineComment]
      ct.table.sourceComment.head.text must_== "This is the users table"
    }

    "parse column-level comments" in {
      val sql = """CREATE TABLE users (
                  |  -- Primary key identifier
                  |  id NUMBER PRIMARY KEY,
                  |  -- User's display name
                  |  name VARCHAR2(100)
                  |)""".stripMargin

      val result = OracleSQL.parseCreateTableWithComments(sql)

      result must beRight
      val ct = result.toOption.get
      ct.table.columns must have size 2

      val idCol = ct.table.columns.find(_.name == "id").get
      idCol.sourceComment must have size 1
      idCol.sourceComment.head.text must_== "Primary key identifier"

      val nameCol = ct.table.columns.find(_.name == "name").get
      nameCol.sourceComment must have size 1
      nameCol.sourceComment.head.text must_== "User's display name"
    }

    "parse block comments" in {
      val sql = """/* Table for storing
                  | * user information
                  | */
                  |CREATE TABLE users (
                  |  id NUMBER
                  |)""".stripMargin

      val result = OracleSQL.parseCreateTableWithComments(sql)

      result must beRight
      val ct = result.toOption.get
      ct.table.sourceComment must have size 1
      ct.table.sourceComment.head must beAnInstanceOf[BlockComment]
    }

    "parse multiple comments on one element" in {
      val sql = """-- First comment
                  |-- Second comment
                  |CREATE TABLE users (
                  |  id NUMBER
                  |)""".stripMargin

      val result = OracleSQL.parseCreateTableWithComments(sql)

      result must beRight
      val ct = result.toOption.get
      // At least the immediately preceding comment is captured
      ct.table.sourceComment must not be empty
      ct.table.sourceComment.map(_.text) must contain("First comment")
    }

    "parse trailing comments on same line" in {
      val sql = """CREATE TABLE users (
                  |  id NUMBER -- column comment
                  |)""".stripMargin

      val result = OracleSQL.parseCreateTableWithComments(sql)

      result must beRight
      val ct = result.toOption.get
      val idCol = ct.table.columns.find(_.name == "id").get
      idCol.sourceComment must not be empty
      idCol.sourceComment.map(_.text) must contain("column comment")
    }

    "produce valid JSON with sourceComment" in {
      val sql = """-- User table
                  |CREATE TABLE users (
                  |  -- User ID
                  |  id NUMBER
                  |)""".stripMargin

      val result = OracleSQL.parseCreateTableWithComments(sql)
      result must beRight

      val json = OracleSQL.statementToJson(result.toOption.get)
      val jsonStr = json.noSpaces

      jsonStr must contain("sourceComment")
      jsonStr must contain("User table")
      jsonStr must contain("User ID")
    }
  }

  "Comment rendering" should {
    "render comments in pretty mode" in {
      val sql = """-- User table
                  |CREATE TABLE users (
                  |  -- Primary key
                  |  id NUMBER,
                  |  -- User name
                  |  name VARCHAR2(100)
                  |)""".stripMargin

      val result = OracleSQL.parseCreateTableWithComments(sql)
      result must beRight
      val ct = result.toOption.get

      given SqlFormatConfig = SqlFormatConfig.Pretty
      val rendered = ct.toSql

      rendered must contain("-- User table")
      rendered must contain("-- Primary key")
      rendered must contain("-- User name")
      rendered must contain("CREATE TABLE")
    }

    "render comments in compact mode as block comments" in {
      val sql = """-- User table
                  |CREATE TABLE users (
                  |  -- Primary key
                  |  id NUMBER,
                  |  -- User name
                  |  name VARCHAR2(100)
                  |)""".stripMargin

      val result = OracleSQL.parseCreateTableWithComments(sql)
      result must beRight
      val ct = result.toOption.get

      given SqlFormatConfig = SqlFormatConfig.Compact
      val rendered = ct.toSql

      rendered must contain("/* User table */")
      rendered must contain("/* Primary key */")
      rendered must contain("/* User name */")
    }

    "round-trip: parse comments, render, verify output" in {
      val sql = """-- Important table
                  |CREATE TABLE products (
                  |  -- Product identifier
                  |  product_id NUMBER
                  |)""".stripMargin

      val result = OracleSQL.parseCreateTableWithComments(sql)
      result must beRight
      val ct = result.toOption.get

      // Verify comments were parsed
      ct.table.sourceComment.map(_.text) must contain("Important table")
      ct.table.columns.head.sourceComment.map(_.text) must contain("Product identifier")

      // Render in pretty mode
      given SqlFormatConfig = SqlFormatConfig.Pretty
      val prettyOutput = ct.toSql

      prettyOutput must contain("-- Important table")
      prettyOutput must contain("-- Product identifier")
      prettyOutput must contain("CREATE TABLE")
      prettyOutput must contain("product_id")
    }
  }
}
