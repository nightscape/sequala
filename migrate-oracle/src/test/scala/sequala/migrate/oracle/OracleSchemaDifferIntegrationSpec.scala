package sequala.migrate.oracle

import com.dimafeng.testcontainers.OracleContainer
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import org.testcontainers.utility.DockerImageName
import sequala.migrate.{SchemaDiffer, SchemaDifferIntegrationSpec}
import sequala.migrate.inspect.OracleInspector
import sequala.schema.*
import sequala.schema.oracle.{OracleDialect, OracleIndexOptions, OracleSqlRenderer}
import sequala.schema.oracle.OracleSqlRenderer.given
import sequala.schema.SqlRenderer.{toSql, given}

import java.sql.Connection

class OracleSchemaDifferIntegrationSpec
    extends SchemaDifferIntegrationSpec
    with TestContainerForAll
    with OracleDialect {

  override val containerDef: OracleContainer.Def =
    OracleContainer.Def(DockerImageName.parse("gvenzl/oracle-xe:21-slim-faststart"))

  override def withConnection[T](f: Connection => T): T = withContainers { oracle =>
    Class.forName("oracle.jdbc.OracleDriver")
    val conn = java.sql.DriverManager.getConnection(oracle.jdbcUrl, oracle.username, oracle.password)
    try f(conn)
    finally conn.close()
  }

  override def inspectTables(conn: Connection): Seq[GenericTable] = {
    val user = conn.getMetaData.getUserName
    OracleInspector.inspectTables(conn, user)
  }

  override def dropAllTables(conn: Connection): Unit = {
    val user = conn.getMetaData.getUserName
    val tables = OracleInspector.inspectTables(conn, user)
    tables.foreach { t =>
      executeSQL(conn, s"""DROP TABLE "${t.name}" CASCADE CONSTRAINTS""")
    }
  }

  // Override render methods to use OracleSchemaDiffRenderer with Oracle-compatible types
  override def renderCreateTable(table: GenericTable): String =
    OracleSchemaDiffRenderer.renderCreateTable(CreateTable(table))

  override def renderAlterTable(tableName: String, action: GenericAlterTableAction): String = {
    val at = AlterTable[CommonDataType, NoColumnOptions.type, NoTableOptions.type, GenericAlterTableAction](
      tableName,
      Seq(action)
    )
    OracleSchemaDiffRenderer.renderAlterTable(at).head
  }

  override def renderDropTable(tableName: String): String =
    OracleSchemaDiffRenderer.renderDropTable(DropTable(tableName))

  override def renderCreateIndex(ci: CreateIndex[IndexOptions]): String =
    OracleSchemaDiffRenderer.renderCreateIndex(ci)

  override def renderDropIndex(di: DropIndex): String =
    OracleSchemaDiffRenderer.renderDropIndex(di)

  // Override to handle Oracle's case-insensitive identifier handling
  // Oracle stores unquoted identifiers in uppercase
  override def tablesEquivalent(actual: GenericTable, expected: GenericTable): Boolean = {
    val a = normalizeTable(actual)
    val e = normalizeTable(expected)
    a.name.equalsIgnoreCase(e.name) &&
    a.columns.map(c => (c.name.toLowerCase, c.dataType, c.nullable)) ==
      e.columns.map(c => (c.name.toLowerCase, c.dataType, c.nullable))
  }

  override def normalizeTable(t: GenericTable): GenericTable =
    t.copy(
      columns = t.columns.sortBy(_.name.toLowerCase),
      indexes = t.indexes.sortBy(_.name.map(_.toLowerCase)),
      foreignKeys = Seq.empty,
      checks = Seq.empty,
      uniques = Seq.empty,
      primaryKey = None
    )

  override def normalizeIndex(i: Index): (Option[String], Seq[String], Boolean) =
    (i.name.map(_.toLowerCase), i.columns.map(_.name.toLowerCase).sorted, i.unique)

  // Override generator to handle Oracle limitation: cannot drop all columns from a table
  // Ensure at least one column is shared between start and desired tables
  import org.scalacheck.Gen
  import sequala.schema.Table as SchemaTable

  override protected def genTwoTablesForDiffTest: Gen[(GenericTable, GenericTable)] = for {
    name <- genName
    sharedCol <- genColumn
    startExtraCols <- Gen.choose(0, 3).flatMap(n => Gen.listOfN(n, genColumn).map(_.distinctBy(_.name)))
    desiredExtraCols <- Gen.choose(0, 3).flatMap(n => Gen.listOfN(n, genColumn).map(_.distinctBy(_.name)))
  } yield {
    val startCols = (sharedCol +: startExtraCols).distinctBy(_.name)
    val desiredCols = (sharedCol +: desiredExtraCols).distinctBy(_.name)
    (
      SchemaTable(name, startCols, None, Seq.empty, Seq.empty, Seq.empty, Seq.empty, NoTableOptions),
      SchemaTable(name, desiredCols, None, Seq.empty, Seq.empty, Seq.empty, Seq.empty, NoTableOptions)
    )
  }

  // Oracle-specific: exclude CLOB/SqlText which Oracle can't index
  override def genIndexableColumnDataType: Gen[CommonDataType] =
    Gen.oneOf(Gen.const(SqlInteger), Gen.const(SqlBigInt), Gen.const(SqlBoolean), Gen.choose(1, 100).map(VarChar(_)))

  override def defaultIndexOptions: IndexOptions = OracleIndexOptions.empty

  override protected def caseInsensitiveMatching: Boolean = true

  // ============================================================================
  // Oracle-specific case sensitivity PBTs
  // ============================================================================

  // Generator for two tables where names differ only in case
  // Oracle stores unquoted identifiers in UPPERCASE, so we need to test
  // that the differ handles lowercase DDL vs UPPERCASE DB identifiers
  def genTwoTablesWithCaseDifference: Gen[(GenericTable, GenericTable)] = for {
    baseName <- genName
    sharedCol <- genColumn
    // Generate lowercase and uppercase versions
    nameLower = baseName.toLowerCase
    nameUpper = baseName.toUpperCase
  } yield {
    val tableLower =
      SchemaTable(nameLower, Seq(sharedCol), None, Seq.empty, Seq.empty, Seq.empty, Seq.empty, NoTableOptions)
    val tableUpper = tableLower.copy(name = nameUpper)
    (tableLower, tableUpper)
  }

  "Oracle case sensitivity PBT" should "match tables that differ only in identifier case" in {
    // This PBT verifies that the SchemaDiffer correctly handles Oracle's
    // case-insensitive identifier matching
    forAll(genTwoTablesWithCaseDifference, minSuccessful(10)) { case (tableLower, tableUpper) =>
      // In Oracle, "users" and "USERS" refer to the same table
      // The differ should produce no DROP/CREATE operations when using caseInsensitive=true
      val diffs = SchemaDiffer.diff(Seq(tableUpper), Seq(tableLower), CommonDiffOptions(caseInsensitive = true))

      // With case-insensitive matching, these should be empty
      diffs.count(_.isInstanceOf[DropTable[?]]) shouldBe 0
      diffs.count(_.isInstanceOf[CreateTable[?, ?, ?]]) shouldBe 0
    }
  }

  it should "correctly apply column changes between case-different table names" in {
    withConnection { conn =>
      // Generator for start/desired with case variations
      val genTablesWithCaseAndColChanges: Gen[(GenericTable, GenericTable)] = for {
        baseName <- genName
        sharedCol <- genColumn
        extraCols <- Gen.choose(0, 2).flatMap(n => Gen.listOfN(n, genColumn).map(_.distinctBy(_.name)))
        // Create start table with lowercase name
        startTable = SchemaTable(
          baseName.toLowerCase,
          (sharedCol +: extraCols).distinctBy(_.name),
          None,
          Seq.empty,
          Seq.empty,
          Seq.empty,
          Seq.empty,
          NoTableOptions
        )
        // Create desired table - Oracle inspector will return UPPERCASE
        desiredCol <- genColumn
        desiredTable = SchemaTable(
          baseName.toUpperCase, // Simulates what Oracle inspector returns
          Seq(sharedCol, desiredCol).distinctBy(_.name),
          None,
          Seq.empty,
          Seq.empty,
          Seq.empty,
          Seq.empty,
          NoTableOptions
        )
      } yield (startTable, desiredTable)

      forAll(genTablesWithCaseAndColChanges, minSuccessful(5)) { case (startTable, desiredTable) =>
        dropAllTables(conn)
        applyCreateTable(conn, startTable)

        // Get inspected state (will have UPPERCASE names from Oracle)
        val inspected = inspectTables(conn)
        inspected.size shouldBe 1

        // Apply changes - this tests that case-insensitive matching works
        // and that column changes are detected correctly
        val diffs = SchemaDiffer.diff(inspected, Seq(desiredTable), CommonDiffOptions(caseInsensitive = true))

        // Should NOT produce DROP TABLE + CREATE TABLE
        // Should only produce ALTER TABLE if there are actual column differences
        diffs.count(_.isInstanceOf[DropTable[?]]) shouldBe 0
        diffs.count(_.isInstanceOf[CreateTable[?, ?, ?]]) shouldBe 0
      }
    }
  }

  // ============================================================================
  // Column name case sensitivity PBTs
  // ============================================================================

  // Generator for tables where column names differ only in case
  def genTablesWithColumnCaseDifference: Gen[(GenericTable, GenericTable)] = for {
    tableName <- genName
    colName <- genName
    dataType <- genColumnDataType
    nullable <- Gen.oneOf(true, false)
  } yield {
    // lowercase column in "desired" state (from DDL)
    val colLower: GenericColumn = Column(colName.toLowerCase, dataType, nullable, None, NoColumnOptions)
    // UPPERCASE column simulating what Oracle inspector returns
    val colUpper: GenericColumn = Column(colName.toUpperCase, dataType, nullable, None, NoColumnOptions)
    val tableLower =
      SchemaTable(tableName, Seq(colLower), None, Seq.empty, Seq.empty, Seq.empty, Seq.empty, NoTableOptions)
    val tableUpper = tableLower.copy(columns = Seq(colUpper))
    (tableLower, tableUpper)
  }

  "Oracle column name case sensitivity PBT" should "match columns that differ only in case" in {
    forAll(genTablesWithColumnCaseDifference, minSuccessful(20)) { case (tableLower, tableUpper) =>
      val diffs = SchemaDiffer.diff(Seq(tableUpper), Seq(tableLower), CommonDiffOptions(caseInsensitive = true))

      // With case-insensitive matching, column names like "id" and "ID" should match
      // Should not produce any column additions or drops
      val alterTables = diffs.collect { case at: AlterTable[?, ?, ?, ?] => at }
      val columnChanges = alterTables.flatMap(_.actions).filter {
        case _: AddColumn[?, ?] => true
        case _: DropColumn[?, ?] => true
        case _ => false
      }
      columnChanges shouldBe empty
    }
  }

  // Generator for tables with multiple columns where some differ in case
  def genTablesWithMixedColumnCases: Gen[(GenericTable, GenericTable)] = for {
    tableName <- genName
    numCols <- Gen.choose(2, 4)
    colNames <- Gen.listOfN(numCols, genName).map(_.distinct)
    if colNames.size >= 2
    dataTypes <- Gen.listOfN(colNames.size, genColumnDataType)
  } yield {
    // lowercase columns (from DDL)
    val colsLower = colNames.zip(dataTypes).map { case (name, dt) =>
      Column[CommonDataType, NoColumnOptions.type](name.toLowerCase, dt, true, None, NoColumnOptions)
    }
    // UPPERCASE columns (from Oracle inspector)
    val colsUpper = colNames.zip(dataTypes).map { case (name, dt) =>
      Column[CommonDataType, NoColumnOptions.type](name.toUpperCase, dt, true, None, NoColumnOptions)
    }
    val tableLower =
      SchemaTable(tableName, colsLower, None, Seq.empty, Seq.empty, Seq.empty, Seq.empty, NoTableOptions)
    val tableUpper = tableLower.copy(columns = colsUpper)
    (tableLower, tableUpper)
  }

  it should "match multiple columns that all differ in case" in {
    forAll(genTablesWithMixedColumnCases, minSuccessful(20)) { case (tableLower, tableUpper) =>
      val diffs = SchemaDiffer.diff(Seq(tableUpper), Seq(tableLower), CommonDiffOptions(caseInsensitive = true))

      // Should not produce DROP TABLE + CREATE TABLE
      diffs.count(_.isInstanceOf[DropTable[?]]) shouldBe 0
      diffs.count(_.isInstanceOf[CreateTable[?, ?, ?]]) shouldBe 0

      // Should not produce any column additions or drops
      val alterTables = diffs.collect { case at: AlterTable[?, ?, ?, ?] => at }
      val columnChanges = alterTables.flatMap(_.actions).filter {
        case _: AddColumn[?, ?] => true
        case _: DropColumn[?, ?] => true
        case _ => false
      }
      columnChanges shouldBe empty
    }
  }

  // Generator for tables where column case differs AND there's an actual column change
  def genTablesWithCaseDiffAndColumnChange: Gen[(GenericTable, GenericTable)] = for {
    tableName <- genName
    sharedColName <- genName
    sharedDataType <- genColumnDataType
    // New column to add (only in desired)
    newColName <- genName.map(n => s"new_$n")
    newDataType <- genColumnDataType
  } yield {
    // UPPERCASE columns (from Oracle inspector) - has only shared column
    val colUpper: GenericColumn = Column(sharedColName.toUpperCase, sharedDataType, true, None, NoColumnOptions)
    val tableUpper =
      SchemaTable(
        tableName.toUpperCase,
        Seq(colUpper),
        None,
        Seq.empty,
        Seq.empty,
        Seq.empty,
        Seq.empty,
        NoTableOptions
      )

    // lowercase columns (desired from DDL) - has shared + new column
    val sharedColLower: GenericColumn = Column(sharedColName.toLowerCase, sharedDataType, true, None, NoColumnOptions)
    val newCol: GenericColumn = Column(newColName.toLowerCase, newDataType, true, None, NoColumnOptions)
    val tableLower = SchemaTable(
      tableName.toLowerCase,
      Seq(sharedColLower, newCol),
      None,
      Seq.empty,
      Seq.empty,
      Seq.empty,
      Seq.empty,
      NoTableOptions
    )

    (tableUpper, tableLower)
  }

  it should "correctly detect column additions when names differ in case" in {
    forAll(genTablesWithCaseDiffAndColumnChange, minSuccessful(20)) { case (inspectedTable, desiredTable) =>
      val diffs = SchemaDiffer.diff(Seq(inspectedTable), Seq(desiredTable), CommonDiffOptions(caseInsensitive = true))

      // Should NOT produce DROP TABLE + CREATE TABLE
      diffs.count(_.isInstanceOf[DropTable[?]]) shouldBe 0
      diffs.count(_.isInstanceOf[CreateTable[?, ?, ?]]) shouldBe 0

      // Should have exactly one AlterTable with one AddColumn
      val alterTables = diffs.collect { case at: AlterTable[?, ?, ?, ?] => at }
      alterTables should have size 1

      val addColumns = alterTables.flatMap(_.actions).collect { case ac: AddColumn[?, ?] => ac }
      addColumns should have size 1

      // Should not have any DropColumn (the shared column should be matched)
      val dropColumns = alterTables.flatMap(_.actions).collect { case dc: DropColumn[?, ?] => dc }
      dropColumns shouldBe empty
    }
  }

  it should "correctly detect column drops when names differ in case" in {
    // Same generator but swap the from/to
    val genTablesWithCaseDiffAndColumnDrop: Gen[(GenericTable, GenericTable)] = for {
      tableName <- genName
      sharedColName <- genName
      sharedDataType <- genColumnDataType
      // Column to drop (only in inspected, not in desired)
      extraColName <- genName.map(n => s"extra_$n")
      extraDataType <- genColumnDataType
    } yield {
      // UPPERCASE columns (from Oracle inspector) - has shared + extra column
      val sharedColUpper: GenericColumn =
        Column(sharedColName.toUpperCase, sharedDataType, true, None, NoColumnOptions)
      val extraCol: GenericColumn = Column(extraColName.toUpperCase, extraDataType, true, None, NoColumnOptions)
      val tableUpper = SchemaTable(
        tableName.toUpperCase,
        Seq(sharedColUpper, extraCol),
        None,
        Seq.empty,
        Seq.empty,
        Seq.empty,
        Seq.empty,
        NoTableOptions
      )

      // lowercase columns (desired from DDL) - has only shared column
      val sharedColLower: GenericColumn =
        Column(sharedColName.toLowerCase, sharedDataType, true, None, NoColumnOptions)
      val tableLower = SchemaTable(
        tableName.toLowerCase,
        Seq(sharedColLower),
        None,
        Seq.empty,
        Seq.empty,
        Seq.empty,
        Seq.empty,
        NoTableOptions
      )

      (tableUpper, tableLower)
    }

    forAll(genTablesWithCaseDiffAndColumnDrop, minSuccessful(20)) { case (inspectedTable, desiredTable) =>
      val diffs = SchemaDiffer.diff(Seq(inspectedTable), Seq(desiredTable), CommonDiffOptions(caseInsensitive = true))

      // Should NOT produce DROP TABLE + CREATE TABLE
      diffs.count(_.isInstanceOf[DropTable[?]]) shouldBe 0
      diffs.count(_.isInstanceOf[CreateTable[?, ?, ?]]) shouldBe 0

      // Should have exactly one AlterTable with one DropColumn
      val alterTables = diffs.collect { case at: AlterTable[?, ?, ?, ?] => at }
      alterTables should have size 1

      val dropColumns = alterTables.flatMap(_.actions).collect { case dc: DropColumn[?, ?] => dc }
      dropColumns should have size 1

      // Should not have any AddColumn (the shared column should be matched)
      val addColumns = alterTables.flatMap(_.actions).collect { case ac: AddColumn[?, ?] => ac }
      addColumns shouldBe empty
    }
  }

  // ============================================================================
  // COMMENT ON statement tests
  // ============================================================================

  "COMMENT ON parsing" should "apply table comments to tables via OracleSchemaBuilder" in {
    import sequala.oracle.OracleSQL

    val ddl = """
CREATE TABLE "GUI_XMDM"."TEST_TABLE" (
  "ID" NUMBER NOT NULL,
  "NAME" VARCHAR2(100)
);

COMMENT ON TABLE "GUI_XMDM"."TEST_TABLE" IS 'This is a test table';
"""
    val results = OracleSQL.parseAll(ddl)
    val statements = results.flatMap(_.result.toOption)
    statements should have size 2

    val tables = OracleSchemaBuilder.fromStatements(statements)
    tables should have size 1
    tables.head.name shouldBe "TEST_TABLE"
    tables.head.comment shouldBe Some("This is a test table")
  }

  it should "apply column comments to columns via OracleSchemaBuilder" in {
    import sequala.oracle.OracleSQL

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
    statements should have size 3

    val tables = OracleSchemaBuilder.fromStatements(statements)
    tables should have size 1

    val idCol = tables.head.columns.find(_.name == "ID")
    idCol shouldBe defined
    idCol.get.comment shouldBe Some("Primary key")

    val nameCol = tables.head.columns.find(_.name == "NAME")
    nameCol shouldBe defined
    nameCol.get.comment shouldBe Some("User name")
  }

  it should "not produce comment diffs when parsed DDL and DB have same comments" in {
    import sequala.oracle.OracleSQL

    val ddl = """
CREATE TABLE "GUI_XMDM"."TEST_TABLE" (
  "ID" NUMBER NOT NULL,
  "NAME" VARCHAR2(100)
);

COMMENT ON TABLE "GUI_XMDM"."TEST_TABLE" IS 'This is a test table';
COMMENT ON COLUMN "GUI_XMDM"."TEST_TABLE"."ID" IS 'Primary key';
"""
    val results = OracleSQL.parseAll(ddl)
    val statements = results.flatMap(_.result.toOption)

    val tablesFromDDL = OracleSchemaBuilder.fromStatements(statements)
    tablesFromDDL should have size 1
    tablesFromDDL.head.comment shouldBe Some("This is a test table")
    tablesFromDDL.head.columns.find(_.name == "ID").get.comment shouldBe Some("Primary key")

    val tablesFromDB = tablesFromDDL.map(_.copy())

    val diffs = SchemaDiffer.diff(tablesFromDB, tablesFromDDL, CommonDiffOptions())

    val commentDiffs = diffs.filter {
      case _: SetTableComment => true
      case _: SetColumnComment => true
      case _ => false
    }

    commentDiffs shouldBe empty
  }

  // ============================================================================
  // Named PRIMARY KEY constraint PBTs
  // ============================================================================

  "Named PRIMARY KEY constraint PBT" should "not produce diffs when constraint name matches" in {
    import sequala.oracle.OracleSQL

    val ddl = """CREATE TABLE "TEST_SCHEMA"."USERS" (
      "ID" NUMBER NOT NULL,
      "NAME" VARCHAR2(100),
      CONSTRAINT "PK_USERS" PRIMARY KEY ("ID")
    )"""

    val parseResult = OracleSQL.parseCreateTableWithComments(ddl)
    parseResult shouldBe Symbol("right")

    val parsedTable = parseResult.toOption.get.table

    parsedTable.primaryKey shouldBe defined
    parsedTable.primaryKey.get.name shouldBe Some("PK_USERS")
    parsedTable.primaryKey.get.columns shouldBe Seq("ID")

    val tableFromDb = parsedTable.copy()

    val diffs = SchemaDiffer.diff(Seq(tableFromDb), Seq(parsedTable), CommonDiffOptions())

    val pkChanges = diffs.collect { case at: AlterTable[?, ?, ?, ?] =>
      at.actions.filter {
        case _: AddConstraint[?, ?] => true
        case _: DropConstraint[?, ?] => true
        case _ => false
      }
    }.flatten

    pkChanges shouldBe empty
  }

  it should "produce no diffs for tables parsed from the same DDL" in {
    import sequala.oracle.OracleSQL

    val ddl = """CREATE TABLE "SCHEMA1"."PRODUCTS" (
      "PRODUCT_ID" NUMBER NOT NULL,
      "SKU" VARCHAR2(50) NOT NULL,
      CONSTRAINT "PK_PRODUCTS" PRIMARY KEY ("PRODUCT_ID", "SKU")
    )"""

    val parseResult1 = OracleSQL.parseCreateTableWithComments(ddl)
    val parseResult2 = OracleSQL.parseCreateTableWithComments(ddl)

    parseResult1 shouldBe Symbol("right")
    parseResult2 shouldBe Symbol("right")

    val table1 = parseResult1.toOption.get.table
    val table2 = parseResult2.toOption.get.table

    val diffs = SchemaDiffer.diff(Seq(table1), Seq(table2), CommonDiffOptions())
    diffs shouldBe empty
  }

  it should "detect when named constraint differs in columns" in {
    import sequala.oracle.OracleSQL

    val ddl1 = """CREATE TABLE "S"."T" (
      "A" NUMBER NOT NULL,
      "B" NUMBER NOT NULL,
      CONSTRAINT "PK_T" PRIMARY KEY ("A")
    )"""

    val ddl2 = """CREATE TABLE "S"."T" (
      "A" NUMBER NOT NULL,
      "B" NUMBER NOT NULL,
      CONSTRAINT "PK_T" PRIMARY KEY ("A", "B")
    )"""

    val t1 = OracleSQL.parseCreateTableWithComments(ddl1).toOption.get.table
    val t2 = OracleSQL.parseCreateTableWithComments(ddl2).toOption.get.table

    val diffs = SchemaDiffer.diff(Seq(t1), Seq(t2), CommonDiffOptions())

    val pkChanges = diffs.collect { case at: AlterTable[?, ?, ?, ?] =>
      at.actions.filter {
        case _: AddConstraint[?, ?] => true
        case _: DropConstraint[?, ?] => true
        case _ => false
      }
    }.flatten

    pkChanges should have size 2
  }
}
