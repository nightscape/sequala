package sequala

import org.specs2.mutable.*
import sequala.oracle.OracleSQL
import sequala.schema.ast.BlockComment

class OracleCommentInstrumentationSpec extends Specification {

  "OracleSQL.parseCreateTableWithInstrumentation" should {

    "capture table comments" in {
      val sql = """-- @TBL_GUI_NAME: Price Monitor
                  |CREATE TABLE SCHEMA.PRICE_MONITOR (
                  |  ID NUMBER NOT NULL
                  |)""".stripMargin

      val result = OracleSQL.parseCreateTableWithInstrumentation(sql)

      result must beRight
      val ct = result.toOption.get
      ct.table.sourceComment must not be empty
      ct.table.sourceComment.map(_.text) must contain("@TBL_GUI_NAME: Price Monitor")
    }

    "capture multiple consecutive table comments" in {
      val sql = """-- @TBL_DISPLAY_ORDER: 3
                  |-- @TBL_GUI_NAME: PRICE TRADE MONITOR PREDEFINED COMMENTS
                  |-- @TBL_NAME_AT: COMMENT_VALUES_AT
                  |-- @TBL_GUI_NAME_SHORT: PREDEFINED COMMENTS
                  |CREATE TABLE "GUI_XMDM_F7"."COMMENT_VALUES" (
                  |  "COL" NUMBER
                  |)""".stripMargin

      val result = OracleSQL.parseCreateTableWithComments(sql)

      result must beRight
      val ct = result.toOption.get
      ct.table.sourceComment must have size 4
      ct.table.sourceComment.map(_.text) must contain("@TBL_DISPLAY_ORDER: 3")
      ct.table.sourceComment.map(_.text) must contain("@TBL_GUI_NAME: PRICE TRADE MONITOR PREDEFINED COMMENTS")
      ct.table.sourceComment.map(_.text) must contain("@TBL_NAME_AT: COMMENT_VALUES_AT")
      ct.table.sourceComment.map(_.text) must contain("@TBL_GUI_NAME_SHORT: PREDEFINED COMMENTS")
    }

    "capture column comments" in {
      val sql = """CREATE TABLE TEST_TABLE (
                  |  -- @COL_PK: Y
                  |  -- @COL_GUI_NAME: Product ID
                  |  PRODUCT_ID NUMBER NOT NULL,
                  |  -- @COL_GUI_NAME: Product Name
                  |  PRODUCT_NAME VARCHAR2(100)
                  |)""".stripMargin

      val result = OracleSQL.parseCreateTableWithInstrumentation(sql)

      result must beRight
      val ct = result.toOption.get
      val columns = ct.table.columns

      columns must have size 2

      val productIdCol = columns.find(_.name == "PRODUCT_ID").get
      productIdCol.sourceComment must not be empty
      productIdCol.sourceComment.map(_.text) must contain("@COL_PK: Y")
      productIdCol.sourceComment.map(_.text) must contain("@COL_GUI_NAME: Product ID")

      val productNameCol = columns.find(_.name == "PRODUCT_NAME").get
      productNameCol.sourceComment must not be empty
      productNameCol.sourceComment.map(_.text) must contain("@COL_GUI_NAME: Product Name")
    }

    "handle block comments" in {
      val sql = """/* Table for storing prices
                  | * with multi-line comment */
                  |CREATE TABLE PRICES (
                  |  /* Primary key column */
                  |  ID NUMBER
                  |)""".stripMargin

      val result = OracleSQL.parseCreateTableWithInstrumentation(sql)

      result must beRight
      val ct = result.toOption.get
      ct.table.sourceComment.exists(_.isInstanceOf[BlockComment]) must beTrue
    }

    "handle mixed comments" in {
      val sql = """-- Line comment for table
                  |/* Block comment */
                  |CREATE TABLE MIXED_COMMENTS (
                  |  -- Column comment
                  |  COL1 NUMBER,
                  |  /* Block column comment */
                  |  COL2 VARCHAR2(50)
                  |)""".stripMargin

      val result = OracleSQL.parseCreateTableWithInstrumentation(sql)

      result must beRight
    }

    "produce equivalent results to parseCreateTableWithComments" in {
      val sql = """-- @TBL_NAME: Test Table
                  |CREATE TABLE TEST (
                  |  -- @COL_PK: Y
                  |  ID NUMBER,
                  |  -- @COL_NAME: Description
                  |  DESCR VARCHAR2(200)
                  |)""".stripMargin

      val instrumentationResult = OracleSQL.parseCreateTableWithInstrumentation(sql)
      val extractorResult = OracleSQL.parseCreateTableWithComments(sql)

      instrumentationResult must beRight
      extractorResult must beRight

      val instCt = instrumentationResult.toOption.get
      val extCt = extractorResult.toOption.get

      instCt.table.sourceComment.map(_.text) must_== extCt.table.sourceComment.map(_.text)

      for (instCol, extCol) <- instCt.table.columns.zip(extCt.table.columns) do {
        instCol.sourceComment.map(_.text) must_== extCol.sourceComment.map(_.text)
      }
      ok
    }
  }
}
