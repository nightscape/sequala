package sequala.migrate

import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import sequala.schema.{Table as SchemaTable, *}
import sequala.schema.GenericSqlRenderer.given
import sequala.schema.SqlRenderer.{toSql, given}

import java.sql.Connection

abstract class SchemaDifferIntegrationSpec extends AnyFlatSpec with Matchers with ScalaCheckPropertyChecks {
  self: DbDialect =>

  def withConnection[T](f: Connection => T): T
  def inspectTables(conn: Connection): Seq[GenericTable]
  def dropAllTables(conn: Connection): Unit

  def defaultIndexOptions: IndexOptions

  def renderCreateTable(table: GenericTable): String = CreateTable(table).toSql

  def renderAlterTable(tableName: String, action: GenericAlterTableAction): String = {
    val at = AlterTable[CommonDataType, NoColumnOptions.type, NoTableOptions.type, GenericAlterTableAction](
      tableName,
      Seq(action)
    )
    at.toSql
  }

  def renderDropTable(tableName: String): String = DropTable(tableName).toSql

  def renderCreateIndex(ci: CreateIndex[IndexOptions]): String = ci.toSql

  def renderDropIndex(di: DropIndex): String = di.toSql

  def renderSetTableComment(stc: SetTableComment): String = {
    val commentStr = stc.comment.map(c => s"'${c.replace("'", "''")}'").getOrElse("NULL")
    s"""COMMENT ON TABLE "${stc.tableName}" IS $commentStr"""
  }

  def renderSetColumnComment(scc: SetColumnComment): String = {
    val commentStr = scc.comment.map(c => s"'${c.replace("'", "''")}'").getOrElse("NULL")
    s"""COMMENT ON COLUMN "${scc.tableName}"."${scc.columnName}" IS $commentStr"""
  }

  val genName: Gen[String] = for {
    first <- Gen.alphaLowerChar
    rest <- Gen.listOfN(5, Gen.alphaLowerChar)
  } yield (first +: rest).mkString

  val genSimpleDataType: Gen[CommonDataType] =
    Gen.oneOf(
      Gen.const(SqlInteger),
      Gen.const(SqlBigInt),
      Gen.const(SqlBoolean),
      Gen.const(SqlText),
      Gen.choose(1, 100).map(VarChar(_))
    )

  def genColumnDataType: Gen[CommonDataType] = genSimpleDataType

  val genComment: Gen[Option[String]] =
    Gen.oneOf(Gen.const(None), Gen.alphaNumStr.map(s => if s.isEmpty then None else Some(s.take(50))))

  def genColumn: Gen[GenericColumn] = for {
    name <- genName
    dataType <- genColumnDataType
    nullable <- Gen.oneOf(true, false)
  } yield Column(name, dataType, nullable, None, NoColumnOptions)

  def genColumnWithComment: Gen[GenericColumn] = for {
    name <- genName
    dataType <- genColumnDataType
    nullable <- Gen.oneOf(true, false)
    comment <- genComment
  } yield Column(name, dataType, nullable, None, NoColumnOptions, comment)

  def genIndexableColumnDataType: Gen[CommonDataType] = genSimpleDataType

  def genIndexableColumn: Gen[GenericColumn] = for {
    name <- genName
    dataType <- genIndexableColumnDataType
    nullable <- Gen.oneOf(true, false)
  } yield Column(name, dataType, nullable, None, NoColumnOptions)

  def genIndexColumn(columnNames: Seq[String]): Gen[IndexColumn] = for {
    name <- Gen.oneOf(columnNames)
    desc <- Gen.oneOf(false, true)
  } yield IndexColumn(name, desc, None)

  def genIndex(columnNames: Seq[String], indexName: String): Gen[Index] = for {
    numCols <- Gen.choose(1, math.min(2, columnNames.size))
    cols <- Gen.pick(numCols, columnNames).map(_.map(IndexColumn(_)).toSeq)
    unique <- Gen.oneOf(false, true)
  } yield Index(Some(indexName), cols, unique, None)

  def genTableWithName(tableName: String): Gen[GenericTable] = for {
    numCols <- Gen.choose(1, 4)
    cols <- Gen.listOfN(numCols, genColumn).map(_.distinctBy(_.name))
    if cols.nonEmpty
  } yield SchemaTable(tableName, cols, None, Seq.empty, Seq.empty, Seq.empty, Seq.empty, NoTableOptions)

  def genTableWithIndexes(tableName: String): Gen[GenericTable] = for {
    numCols <- Gen.choose(2, 4)
    cols <- Gen.listOfN(numCols, genIndexableColumn).map(_.distinctBy(_.name))
    if cols.size >= 2
    colNames = cols.map(_.name)
    numIndexes <- Gen.choose(0, 2)
    indexNames <- Gen.listOfN(numIndexes, genName).map(_.distinct.map(n => s"idx_${tableName}_$n"))
    indexes <- genDistinctIndexes(colNames, indexNames)
  } yield SchemaTable(tableName, cols, None, indexes, Seq.empty, Seq.empty, Seq.empty, NoTableOptions)

  val genTable: Gen[GenericTable] = for {
    name <- genName
    table <- genTableWithName(name)
  } yield table

  def executeSQL(conn: Connection, sql: String): Unit = {
    val stmt = conn.createStatement()
    stmt.execute(sql)
    stmt.close()
  }

  def applyCreateTable(conn: Connection, table: GenericTable): Unit = {
    val sql = renderCreateTable(table)
    executeSQL(conn, sql)
  }

  def applyDiff(conn: Connection, diffs: Seq[SchemaDiffOp]): Unit = {
    diffs.foreach {
      case ct: CreateTable[?, ?, ?] =>
        val sql = renderCreateTable(ct.table.asInstanceOf[GenericTable])
        executeSQL(conn, sql)
      case dt: DropTable[?] =>
        val sql = renderDropTable(dt.tableName)
        executeSQL(conn, sql)
      case at: AlterTable[?, ?, ?, ?] =>
        at.actions.foreach { action =>
          val sql = renderAlterTable(at.tableName, action.asInstanceOf[GenericAlterTableAction])
          executeSQL(conn, sql)
        }
      case ci: CreateIndex[IndexOptions @unchecked] =>
        val sql = renderCreateIndex(ci)
        executeSQL(conn, sql)
      case di: DropIndex =>
        val sql = renderDropIndex(di)
        executeSQL(conn, sql)
      case stc: SetTableComment =>
        val sql = renderSetTableComment(stc)
        executeSQL(conn, sql)
      case scc: SetColumnComment =>
        val sql = renderSetColumnComment(scc)
        executeSQL(conn, sql)
    }
  }

  protected def caseInsensitiveMatching: Boolean = false

  protected def applyDesiredSchema(
    conn: Connection,
    currentTables: Seq[GenericTable],
    desiredTables: Seq[GenericTable]
  ): Unit = {
    val diffs =
      SchemaDiffer.diff(currentTables, desiredTables, CommonDiffOptions(caseInsensitive = caseInsensitiveMatching))
    applyDiff(conn, diffs)
  }

  protected def generateTablesDDL(tables: Seq[GenericTable]): String = {
    if tables.isEmpty then ";"
    else {
      val tablesDDL = tables.map(t => renderCreateTable(t))
      val indexesDDL = tables.flatMap { t =>
        t.indexes.flatMap(_.name).map { indexName =>
          val idx = t.indexes.find(_.name.contains(indexName)).get
          renderCreateIndex(CreateIndex(indexName, t.name, idx.columns, idx.unique, false, defaultIndexOptions))
        }
      }
      (tablesDDL ++ indexesDDL).mkString(";\n\n") + ";"
    }
  }

  def normalizeTable(t: GenericTable): GenericTable =
    t.copy(
      columns = t.columns.sortBy(_.name),
      indexes = t.indexes.sortBy(_.name),
      foreignKeys = Seq.empty,
      checks = Seq.empty,
      uniques = Seq.empty,
      primaryKey = None
    )

  def normalizeIndex(i: Index): (Option[String], Seq[String], Boolean) =
    (i.name, i.columns.map(_.name).sorted, i.unique)

  def tablesEquivalent(actual: GenericTable, expected: GenericTable): Boolean = {
    val a = normalizeTable(actual)
    val e = normalizeTable(expected)
    a.name == e.name &&
    a.columns.map(c => (c.name, c.dataType, c.nullable)) ==
      e.columns.map(c => (c.name, c.dataType, c.nullable))
  }

  def indexesEquivalent(actual: Seq[Index], expected: Seq[Index]): Boolean = {
    val normalizedActual = actual.map(normalizeIndex).toSet
    val normalizedExpected = expected.map(normalizeIndex).toSet
    normalizedActual == normalizedExpected
  }

  def tablesWithIndexesEquivalent(actual: GenericTable, expected: GenericTable): Boolean =
    tablesEquivalent(actual, expected) && indexesEquivalent(actual.indexes, expected.indexes)

  "SchemaDiffer" should "correctly add a new table" in {
    withConnection { conn =>
      dropAllTables(conn)

      forAll(genTable, minSuccessful(5)) { generatedTable =>
        dropAllTables(conn)

        applyDesiredSchema(conn, Seq.empty, Seq(generatedTable))

        val inspected = inspectTables(conn)
        inspected.size shouldBe 1
        tablesEquivalent(inspected.head, generatedTable) shouldBe true
      }
    }
  }

  it should "correctly drop a table" in {
    withConnection { conn =>
      forAll(genTable, minSuccessful(5)) { generatedTable =>
        dropAllTables(conn)
        applyCreateTable(conn, generatedTable)

        applyDesiredSchema(conn, Seq(generatedTable), Seq.empty)

        val inspected = inspectTables(conn)
        inspected shouldBe empty
      }
    }
  }

  it should "correctly add a column to existing table" in {
    withConnection { conn =>
      val genTableAndNewColumn = for {
        generatedTable <- genTable
        newCol <- genColumn.map(c => c.copy(name = s"new_${c.name}"))
      } yield (generatedTable, newCol)

      forAll(genTableAndNewColumn, minSuccessful(5)) { case (generatedTable, newCol) =>
        dropAllTables(conn)
        applyCreateTable(conn, generatedTable)

        val modifiedTable = generatedTable.copy(columns = generatedTable.columns :+ newCol)
        applyDesiredSchema(conn, Seq(generatedTable), Seq(modifiedTable))

        val inspected = inspectTables(conn)
        inspected.size shouldBe 1
        tablesEquivalent(inspected.head, modifiedTable) shouldBe true
      }
    }
  }

  it should "correctly drop a column from existing table" in {
    withConnection { conn =>
      val genTableWithMultipleCols = for {
        name <- genName
        numCols <- Gen.choose(2, 4)
        cols <- Gen.listOfN(numCols, genColumn).map(_.distinctBy(_.name))
        if cols.size >= 2
      } yield SchemaTable(name, cols, None, Seq.empty, Seq.empty, Seq.empty, Seq.empty, NoTableOptions)

      forAll(genTableWithMultipleCols, minSuccessful(5)) { generatedTable =>
        dropAllTables(conn)
        applyCreateTable(conn, generatedTable)

        val modifiedTable = generatedTable.copy(columns = generatedTable.columns.tail)
        applyDesiredSchema(conn, Seq(generatedTable), Seq(modifiedTable))

        val inspected = inspectTables(conn)
        inspected.size shouldBe 1
        tablesEquivalent(inspected.head, modifiedTable) shouldBe true
      }
    }
  }

  // Protected generator that can be overridden (e.g., for Oracle which can't drop all columns)
  protected def genTwoTablesForDiffTest: Gen[(GenericTable, GenericTable)] = for {
    name <- genName
    start <- genTableWithName(name)
    desired <- genTableWithName(name)
  } yield (start, desired)

  it should "produce identical schema when diffing and applying" in {
    withConnection { conn =>
      forAll(genTwoTablesForDiffTest, minSuccessful(10)) { case (startTable, desiredTable) =>
        dropAllTables(conn)
        applyCreateTable(conn, startTable)

        applyDesiredSchema(conn, Seq(startTable), Seq(desiredTable))

        val inspected = inspectTables(conn)
        inspected.size shouldBe 1
        tablesEquivalent(inspected.head, desiredTable) shouldBe true
      }
    }
  }

  protected def genTableWithIndexesForTest(tableName: String): Gen[GenericTable] =
    genTableWithIndexes(tableName)

  it should "correctly create a table with indexes" in {
    withConnection { conn =>
      val genTableWithIdx = for {
        name <- genName
        table <- genTableWithIndexesForTest(name)
      } yield table

      forAll(genTableWithIdx, minSuccessful(5)) { table =>
        dropAllTables(conn)

        applyDesiredSchema(conn, Seq.empty, Seq(table))

        val inspected = inspectTables(conn)
        inspected.size shouldBe 1
        tablesWithIndexesEquivalent(inspected.head, table) shouldBe true
      }
    }
  }

  it should "correctly add an index to existing table" in {
    withConnection { conn =>
      val genTableAndIndex = for {
        name <- genName
        numCols <- Gen.choose(2, 4)
        cols <- Gen.listOfN(numCols, genIndexableColumn).map(_.distinctBy(_.name))
        if cols.size >= 2
        colNames = cols.map(_.name)
        indexName <- genName.map(n => s"idx_${name}_$n")
        index <- genIndex(colNames, indexName)
        tableNoIndex = SchemaTable(name, cols, None, Seq.empty, Seq.empty, Seq.empty, Seq.empty, NoTableOptions)
        tableWithIndex = tableNoIndex.copy(indexes = Seq(index))
      } yield (tableNoIndex, tableWithIndex)

      forAll(genTableAndIndex, minSuccessful(5)) { case (startTable, desiredTable) =>
        dropAllTables(conn)
        applyCreateTable(conn, startTable)

        applyDesiredSchema(conn, Seq(startTable), Seq(desiredTable))

        val inspected = inspectTables(conn)
        inspected.size shouldBe 1
        tablesWithIndexesEquivalent(inspected.head, desiredTable) shouldBe true
      }
    }
  }

  it should "correctly drop an index from existing table" in {
    withConnection { conn =>
      val genTableWithAndWithoutIndex = for {
        name <- genName
        numCols <- Gen.choose(2, 4)
        cols <- Gen.listOfN(numCols, genIndexableColumn).map(_.distinctBy(_.name))
        if cols.size >= 2
        colNames = cols.map(_.name)
        indexName <- genName.map(n => s"idx_${name}_$n")
        index <- genIndex(colNames, indexName)
        tableWithIndex = SchemaTable(name, cols, None, Seq(index), Seq.empty, Seq.empty, Seq.empty, NoTableOptions)
        tableNoIndex = tableWithIndex.copy(indexes = Seq.empty)
      } yield (tableWithIndex, tableNoIndex)

      forAll(genTableWithAndWithoutIndex, minSuccessful(5)) { case (startTable, desiredTable) =>
        dropAllTables(conn)
        applyCreateTable(conn, startTable)
        startTable.indexes.foreach { idx =>
          idx.name.foreach { name =>
            executeSQL(
              conn,
              renderCreateIndex(CreateIndex(name, startTable.name, idx.columns, idx.unique, false, defaultIndexOptions))
            )
          }
        }

        applyDesiredSchema(conn, Seq(startTable), Seq(desiredTable))

        val inspected = inspectTables(conn)
        inspected.size shouldBe 1
        tablesWithIndexesEquivalent(inspected.head, desiredTable) shouldBe true
      }
    }
  }

  protected def genTwoTablesWithIndexesForDiffTest: Gen[(GenericTable, GenericTable)] = for {
    name <- genName
    numCols <- Gen.choose(2, 4)
    cols <- Gen.listOfN(numCols, genIndexableColumn).map(_.distinctBy(_.name))
    if cols.size >= 2
    colNames = cols.map(_.name)
    startNumIndexes <- Gen.choose(0, math.min(2, colNames.size))
    startIndexNames <- Gen.listOfN(startNumIndexes, genName).map(_.distinct.map(n => s"idx_${name}_$n"))
    startIndexes <- genDistinctIndexes(colNames, startIndexNames)
    desiredNumIndexes <- Gen.choose(0, math.min(2, colNames.size))
    desiredIndexNames <- Gen.listOfN(desiredNumIndexes, genName).map(_.distinct.map(n => s"idx_${name}_$n"))
    desiredIndexes <- genDistinctIndexes(colNames, desiredIndexNames)
    startTable = SchemaTable(name, cols, None, startIndexes, Seq.empty, Seq.empty, Seq.empty, NoTableOptions)
    desiredTable = startTable.copy(indexes = desiredIndexes)
  } yield (startTable, desiredTable)

  def genDistinctIndexes(colNames: Seq[String], indexNames: Seq[String]): Gen[Seq[Index]] =
    if indexNames.isEmpty then Gen.const(Seq.empty)
    else
      val colSubsets = (1 to colNames.size).flatMap(n => colNames.combinations(n).toSeq).distinct
      for {
        shuffled <- Gen.pick(math.min(indexNames.size, colSubsets.size), colSubsets)
        indexes <- Gen.sequence[Seq[Index], Index](indexNames.zip(shuffled).map { case (name, cols) =>
          Gen.oneOf(true, false).map(unique => Index(Some(name), cols.map(IndexColumn(_)).toSeq, unique, None))
        })
      } yield indexes

  it should "produce identical indexes when diffing and applying" in {
    withConnection { conn =>
      forAll(genTwoTablesWithIndexesForDiffTest, minSuccessful(10)) { case (startTable, desiredTable) =>
        dropAllTables(conn)
        applyCreateTable(conn, startTable)
        startTable.indexes.foreach { idx =>
          idx.name.foreach { name =>
            executeSQL(
              conn,
              renderCreateIndex(CreateIndex(name, startTable.name, idx.columns, idx.unique, false, defaultIndexOptions))
            )
          }
        }

        applyDesiredSchema(conn, Seq(startTable), Seq(desiredTable))

        val inspected = inspectTables(conn)
        inspected.size shouldBe 1
        tablesWithIndexesEquivalent(inspected.head, desiredTable) shouldBe true
      }
    }
  }

  // Comment-related generators and tests

  def genTableWithComments(tableName: String): Gen[GenericTable] = for {
    numCols <- Gen.choose(1, 4)
    cols <- Gen.listOfN(numCols, genColumnWithComment).map(_.distinctBy(_.name))
    if cols.nonEmpty
    tableComment <- genComment
  } yield SchemaTable(tableName, cols, None, Seq.empty, Seq.empty, Seq.empty, Seq.empty, NoTableOptions, tableComment)

  def commentsEquivalent(actual: GenericTable, expected: GenericTable): Boolean = {
    val tableCommentMatch = actual.comment == expected.comment
    val columnCommentsMatch = actual.columns.sortBy(_.name).map(c => (c.name, c.comment)) ==
      expected.columns.sortBy(_.name).map(c => (c.name, c.comment))
    tableCommentMatch && columnCommentsMatch
  }

  def tablesWithCommentsEquivalent(actual: GenericTable, expected: GenericTable): Boolean =
    tablesEquivalent(actual, expected) && commentsEquivalent(actual, expected)

  it should "detect table comment changes" in {
    val fromTable = SchemaTable(
      "test_table",
      Seq(Column("id", SqlInteger, false, None, NoColumnOptions)),
      None,
      Seq.empty,
      Seq.empty,
      Seq.empty,
      Seq.empty,
      NoTableOptions,
      comment = Some("old comment")
    )
    val toTable = fromTable.copy(comment = Some("new comment"))

    val diffs = SchemaDiffer.diff(Seq(fromTable), Seq(toTable))

    diffs should have size 1
    diffs.head shouldBe a[SetTableComment]
    val stc = diffs.head.asInstanceOf[SetTableComment]
    stc.tableName shouldBe "test_table"
    stc.comment shouldBe Some("new comment")
  }

  it should "detect column comment changes" in {
    val fromTable = SchemaTable(
      "test_table",
      Seq(Column("id", SqlInteger, false, None, NoColumnOptions, Some("old comment"))),
      None,
      Seq.empty,
      Seq.empty,
      Seq.empty,
      Seq.empty,
      NoTableOptions
    )
    val toTable =
      fromTable.copy(columns = Seq(Column("id", SqlInteger, false, None, NoColumnOptions, Some("new comment"))))

    val diffs = SchemaDiffer.diff(Seq(fromTable), Seq(toTable))

    diffs should have size 1
    diffs.head shouldBe a[SetColumnComment]
    val scc = diffs.head.asInstanceOf[SetColumnComment]
    scc.tableName shouldBe "test_table"
    scc.columnName shouldBe "id"
    scc.comment shouldBe Some("new comment")
  }

  it should "detect adding a table comment" in {
    val fromTable = SchemaTable(
      "test_table",
      Seq(Column("id", SqlInteger, false, None, NoColumnOptions)),
      None,
      Seq.empty,
      Seq.empty,
      Seq.empty,
      Seq.empty,
      NoTableOptions,
      comment = None
    )
    val toTable = fromTable.copy(comment = Some("new comment"))

    val diffs = SchemaDiffer.diff(Seq(fromTable), Seq(toTable))

    diffs should have size 1
    diffs.head shouldBe a[SetTableComment]
  }

  it should "detect removing a table comment" in {
    val fromTable = SchemaTable(
      "test_table",
      Seq(Column("id", SqlInteger, false, None, NoColumnOptions)),
      None,
      Seq.empty,
      Seq.empty,
      Seq.empty,
      Seq.empty,
      NoTableOptions,
      comment = Some("old comment")
    )
    val toTable = fromTable.copy(comment = None)

    val diffs = SchemaDiffer.diff(Seq(fromTable), Seq(toTable))

    diffs should have size 1
    diffs.head shouldBe a[SetTableComment]
    diffs.head.asInstanceOf[SetTableComment].comment shouldBe None
  }

  it should "not generate diffs when comments are identical" in {
    val table = SchemaTable(
      "test_table",
      Seq(Column("id", SqlInteger, false, None, NoColumnOptions, Some("same comment"))),
      None,
      Seq.empty,
      Seq.empty,
      Seq.empty,
      Seq.empty,
      NoTableOptions,
      comment = Some("table comment")
    )

    val diffs = SchemaDiffer.diff(Seq(table), Seq(table))

    diffs shouldBe empty
  }

  protected def genTwoTablesWithCommentsForDiffTest: Gen[(GenericTable, GenericTable)] = for {
    name <- genName
    numCols <- Gen.choose(1, 3)
    colNames <- Gen.listOfN(numCols, genName).map(_.distinct)
    if colNames.nonEmpty
    startComments <- Gen.listOfN(colNames.size, genComment)
    desiredComments <- Gen.listOfN(colNames.size, genComment)
    startTableComment <- genComment
    desiredTableComment <- genComment
    cols = colNames.zip(startComments).map { case (n, c) =>
      Column[CommonDataType, NoColumnOptions.type](n, SqlInteger, true, None, NoColumnOptions, c)
    }
    startTable = SchemaTable(
      name,
      cols,
      None,
      Seq.empty,
      Seq.empty,
      Seq.empty,
      Seq.empty,
      NoTableOptions,
      startTableComment
    )
    desiredCols = colNames.zip(desiredComments).map { case (n, c) =>
      Column[CommonDataType, NoColumnOptions.type](n, SqlInteger, true, None, NoColumnOptions, c)
    }
    desiredTable = SchemaTable(
      name,
      desiredCols,
      None,
      Seq.empty,
      Seq.empty,
      Seq.empty,
      Seq.empty,
      NoTableOptions,
      desiredTableComment
    )
  } yield (startTable, desiredTable)

  it should "correctly diff comments with property-based testing" in {
    forAll(genTwoTablesWithCommentsForDiffTest, minSuccessful(20)) { case (startTable, desiredTable) =>
      val diffs = SchemaDiffer.diff(Seq(startTable), Seq(desiredTable))

      // Count expected comment diffs
      val expectedTableCommentDiff = if startTable.comment != desiredTable.comment then 1 else 0
      val expectedColumnCommentDiffs = startTable.columns.zip(desiredTable.columns).count { case (from, to) =>
        from.comment != to.comment
      }

      val actualTableCommentDiffs = diffs.count(_.isInstanceOf[SetTableComment])
      val actualColumnCommentDiffs = diffs.count(_.isInstanceOf[SetColumnComment])

      actualTableCommentDiffs shouldBe expectedTableCommentDiff
      actualColumnCommentDiffs shouldBe expectedColumnCommentDiffs

      // Verify the diff contents are correct
      diffs.collect { case stc: SetTableComment => stc }.foreach { stc =>
        stc.tableName shouldBe desiredTable.name
        stc.comment shouldBe desiredTable.comment
      }

      diffs.collect { case scc: SetColumnComment => scc }.foreach { scc =>
        scc.tableName shouldBe desiredTable.name
        val desiredCol = desiredTable.columns.find(_.name == scc.columnName)
        desiredCol shouldBe defined
        scc.comment shouldBe desiredCol.get.comment
      }
    }
  }

  // ============================================================================
  // Schema-qualified table name PBTs
  // ============================================================================

  /** Generator for table names with optional schema qualification */
  def genSchemaQualifiedName: Gen[(Option[String], String)] = for {
    hasSchema <- Gen.oneOf(true, false)
    schemaName <- if hasSchema then genName.map(Some(_)) else Gen.const(None)
    tableName <- genName
  } yield (schemaName, tableName)

  /** Generate a table with explicit schema field set */
  def genTableWithSchema(schemaOpt: Option[String], tableName: String): Gen[GenericTable] = for {
    numCols <- Gen.choose(1, 4)
    cols <- Gen.listOfN(numCols, genColumn).map(_.distinctBy(_.name))
    if cols.nonEmpty
  } yield SchemaTable(
    tableName,
    cols,
    None,
    Seq.empty,
    Seq.empty,
    Seq.empty,
    Seq.empty,
    NoTableOptions,
    None,
    schemaOpt
  )

  /** Generator for pairs of tables with the same schema and name (should match) */
  def genMatchingTablesWithSchema: Gen[(GenericTable, GenericTable)] = for {
    (schemaOpt, tableName) <- genSchemaQualifiedName
    sharedCol <- genColumn
  } yield {
    val table1 = SchemaTable(
      tableName,
      Seq(sharedCol),
      None,
      Seq.empty,
      Seq.empty,
      Seq.empty,
      Seq.empty,
      NoTableOptions,
      None,
      schemaOpt
    )
    (table1, table1.copy())
  }

  /** Generator for pairs of tables with different schemas (should NOT match) */
  def genTablesWithDifferentSchemas: Gen[(GenericTable, GenericTable)] = for {
    tableName <- genName
    schema1 <- genName
    schema2 <- genName.suchThat(_ != schema1)
    col <- genColumn
  } yield {
    val table1 = SchemaTable(
      tableName,
      Seq(col),
      None,
      Seq.empty,
      Seq.empty,
      Seq.empty,
      Seq.empty,
      NoTableOptions,
      None,
      schema = Some(schema1)
    )
    val table2 = table1.copy(schema = Some(schema2))
    (table1, table2)
  }

  /** Generator for pairs where one has schema and one doesn't (should NOT match) */
  def genTablesSchemaVsNoSchema: Gen[(GenericTable, GenericTable)] = for {
    tableName <- genName
    schemaName <- genName
    col <- genColumn
  } yield {
    val tableWithSchema = SchemaTable(
      tableName,
      Seq(col),
      None,
      Seq.empty,
      Seq.empty,
      Seq.empty,
      Seq.empty,
      NoTableOptions,
      None,
      schema = Some(schemaName)
    )
    val tableWithoutSchema = tableWithSchema.copy(schema = None)
    (tableWithSchema, tableWithoutSchema)
  }

  "Schema-qualified table matching PBT" should "produce no diffs for tables with same schema and name" in {
    forAll(genMatchingTablesWithSchema, minSuccessful(20)) { case (table1, table2) =>
      val diffs = SchemaDiffer.diff(Seq(table1), Seq(table2))
      diffs shouldBe empty
    }
  }

  it should "produce DROP+CREATE for tables with different schemas" in {
    forAll(genTablesWithDifferentSchemas, minSuccessful(20)) { case (table1, table2) =>
      val diffs = SchemaDiffer.diff(Seq(table1), Seq(table2))

      // Should have both a drop and a create since they're different tables
      diffs.count(_.isInstanceOf[DropTable[?]]) shouldBe 1
      diffs.count(_.isInstanceOf[CreateTable[?, ?, ?]]) shouldBe 1
    }
  }

  it should "produce DROP+CREATE for table with schema vs table without schema" in {
    forAll(genTablesSchemaVsNoSchema, minSuccessful(20)) { case (tableWithSchema, tableWithoutSchema) =>
      val diffs = SchemaDiffer.diff(Seq(tableWithSchema), Seq(tableWithoutSchema))

      // Should have both a drop and a create since qualified names differ
      diffs.count(_.isInstanceOf[DropTable[?]]) shouldBe 1
      diffs.count(_.isInstanceOf[CreateTable[?, ?, ?]]) shouldBe 1
    }
  }

  it should "generate correct qualifiedName for schema-qualified tables" in {
    forAll(genSchemaQualifiedName, minSuccessful(20)) { case (schemaOpt, tableName) =>
      val table = SchemaTable(
        tableName,
        Seq(Column("id", SqlInteger, false, None, NoColumnOptions)),
        None,
        Seq.empty,
        Seq.empty,
        Seq.empty,
        Seq.empty,
        NoTableOptions,
        None,
        schemaOpt
      )

      schemaOpt match {
        case Some(schema) => table.qualifiedName shouldBe s"$schema.$tableName"
        case None => table.qualifiedName shouldBe tableName
      }
    }
  }

  protected def genTwoTablesWithSchemaForDiffTest: Gen[(GenericTable, GenericTable)] = for {
    (schemaOpt, tableName) <- genSchemaQualifiedName
    start <- genTableWithSchema(schemaOpt, tableName)
    desired <- genTableWithSchema(schemaOpt, tableName)
  } yield (start, desired)

  it should "correctly apply column changes to schema-qualified tables" in {
    // This PBT verifies that column modifications work correctly on schema-qualified tables
    val genTablesWithSchemaAndColChanges: Gen[(GenericTable, GenericTable)] = for {
      (schemaOpt, tableName) <- genSchemaQualifiedName
      sharedCol <- genColumn
      extraStartCols <- Gen.choose(0, 2).flatMap(n => Gen.listOfN(n, genColumn).map(_.distinctBy(_.name)))
      extraDesiredCols <- Gen.choose(0, 2).flatMap(n => Gen.listOfN(n, genColumn).map(_.distinctBy(_.name)))
    } yield {
      val startCols = (sharedCol +: extraStartCols).distinctBy(_.name)
      val desiredCols = (sharedCol +: extraDesiredCols).distinctBy(_.name)
      (
        SchemaTable(
          tableName,
          startCols,
          None,
          Seq.empty,
          Seq.empty,
          Seq.empty,
          Seq.empty,
          NoTableOptions,
          None,
          schemaOpt
        ),
        SchemaTable(
          tableName,
          desiredCols,
          None,
          Seq.empty,
          Seq.empty,
          Seq.empty,
          Seq.empty,
          NoTableOptions,
          None,
          schemaOpt
        )
      )
    }

    forAll(genTablesWithSchemaAndColChanges, minSuccessful(20)) { case (startTable, desiredTable) =>
      val diffs = SchemaDiffer.diff(Seq(startTable), Seq(desiredTable))

      // Should NOT produce DROP TABLE + CREATE TABLE
      // Should only produce ALTER TABLE for column changes
      diffs.count(_.isInstanceOf[DropTable[?]]) shouldBe 0
      diffs.count(_.isInstanceOf[CreateTable[?, ?, ?]]) shouldBe 0

      // If columns differ, should have AlterTable
      if startTable.columns.map(_.name).toSet != desiredTable.columns.map(_.name).toSet then {
        diffs.count(_.isInstanceOf[AlterTable[?, ?, ?, ?]]) should be >= 1
      }
    }
  }

  // ============================================================================
  // Data preservation PBTs
  // ============================================================================

  /** Insert test data into a table */
  protected def insertTestData(conn: Connection, tableName: String, columnName: String, value: Int): Unit = {
    executeSQL(conn, s"""INSERT INTO "$tableName" ("$columnName") VALUES ($value)""")
  }

  /** Count rows in a table */
  protected def countRows(conn: Connection, tableName: String): Int = {
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(s"""SELECT COUNT(*) FROM "$tableName"""")
    rs.next()
    val count = rs.getInt(1)
    rs.close()
    stmt.close()
    count
  }

  /** Generator for number of rows to insert (between 1 and 5) */
  val genRowCount: Gen[Int] = Gen.choose(1, 5)

  /** Generator for an integer column (for data insertion tests) */
  def genIntegerColumn: Gen[GenericColumn] = for {
    name <- genName
  } yield Column[CommonDataType, NoColumnOptions.type](name, SqlInteger, true, None, NoColumnOptions)

  /** Generator for table with a column that can be added to */
  def genTableForAddColumn: Gen[(GenericTable, GenericColumn)] = for {
    tableName <- genName
    existingColName <- genName
    newCol <- genColumn.map(c =>
      Column[CommonDataType, NoColumnOptions.type](s"new_${c.name}", c.dataType, true, None, NoColumnOptions)
    )
  } yield {
    val existingCol: GenericColumn = Column(existingColName, SqlInteger, true, None, NoColumnOptions)
    val table =
      SchemaTable(tableName, Seq(existingCol), None, Seq.empty, Seq.empty, Seq.empty, Seq.empty, NoTableOptions)
    (table, newCol)
  }

  /** Generator for table with columns where one can be dropped */
  def genTableForDropColumn: Gen[(GenericTable, String)] = for {
    tableName <- genName
    keepColName <- genName
    dropColName <- genName.map(n => s"drop_$n")
  } yield {
    val keepCol: GenericColumn = Column(keepColName, SqlInteger, true, None, NoColumnOptions)
    val dropCol: GenericColumn = Column(dropColName, VarChar(100), true, None, NoColumnOptions)
    val table =
      SchemaTable(tableName, Seq(keepCol, dropCol), None, Seq.empty, Seq.empty, Seq.empty, Seq.empty, NoTableOptions)
    (table, dropColName)
  }

  "Data preservation PBT" should "preserve data when adding a column" in {
    withConnection { conn =>
      forAll(genTableForAddColumn, genRowCount, minSuccessful(5)) { case ((initialTable, newCol), rowCount) =>
        dropAllTables(conn)
        applyCreateTable(conn, initialTable)

        // Insert test data
        val existingColName = initialTable.columns.head.name
        (1 to rowCount).foreach { i =>
          insertTestData(conn, initialTable.name, existingColName, i * 10)
        }
        countRows(conn, initialTable.name) shouldBe rowCount

        // Create desired table with additional column
        val desiredTable = initialTable.copy(columns = initialTable.columns :+ newCol)

        // Apply the diff
        applyDesiredSchema(conn, Seq(initialTable), Seq(desiredTable))

        // Verify data is preserved (this would fail if DROP+CREATE was used)
        countRows(conn, initialTable.name) shouldBe rowCount

        // Verify schema is correct
        val inspected = inspectTables(conn)
        inspected.size shouldBe 1
        tablesEquivalent(inspected.head, desiredTable) shouldBe true
      }
    }
  }

  it should "preserve data when dropping a column" in {
    withConnection { conn =>
      forAll(genTableForDropColumn, genRowCount, minSuccessful(5)) { case ((initialTable, colToDrop), rowCount) =>
        dropAllTables(conn)
        applyCreateTable(conn, initialTable)

        // Insert test data into the column we're keeping
        val keepColName = initialTable.columns.find(_.name != colToDrop).get.name
        (1 to rowCount).foreach { i =>
          insertTestData(conn, initialTable.name, keepColName, i * 10)
        }
        countRows(conn, initialTable.name) shouldBe rowCount

        // Create desired table without the column to drop
        val desiredTable = initialTable.copy(columns = initialTable.columns.filterNot(_.name == colToDrop))

        // Apply the diff
        applyDesiredSchema(conn, Seq(initialTable), Seq(desiredTable))

        // Verify data is preserved
        countRows(conn, initialTable.name) shouldBe rowCount

        // Verify schema is correct
        val inspected = inspectTables(conn)
        inspected.size shouldBe 1
        tablesEquivalent(inspected.head, desiredTable) shouldBe true
      }
    }
  }

  it should "generate AlterTable instead of DropTable+CreateTable for column changes" in {
    // Generator for table pairs where columns differ
    val genTablePairWithColumnChanges: Gen[(GenericTable, GenericTable)] = for {
      tableName <- genName
      sharedCol <- genColumn
      addedCol <- genColumn.map(c => c.copy(name = s"added_${c.name}"))
    } yield {
      val tableBefore =
        SchemaTable(tableName, Seq(sharedCol), None, Seq.empty, Seq.empty, Seq.empty, Seq.empty, NoTableOptions)
      val tableAfter = tableBefore.copy(columns = Seq(sharedCol, addedCol))
      (tableBefore, tableAfter)
    }

    forAll(genTablePairWithColumnChanges, minSuccessful(20)) { case (tableBefore, tableAfter) =>
      val diffs = SchemaDiffer.diff(Seq(tableBefore), Seq(tableAfter))

      // Should produce AlterTable, NOT DropTable + CreateTable
      diffs.count(_.isInstanceOf[DropTable[?]]) shouldBe 0
      diffs.count(_.isInstanceOf[CreateTable[?, ?, ?]]) shouldBe 0
      diffs.count(_.isInstanceOf[AlterTable[?, ?, ?, ?]]) shouldBe 1

      val alterTable = diffs.head.asInstanceOf[AlterTable[?, ?, ?, ?]]
      alterTable.actions.size shouldBe 1
      alterTable.actions.head shouldBe a[AddColumn[?, ?]]
    }
  }

  it should "generate AlterTable for column removal" in {
    val genTablePairWithColumnRemoval: Gen[(GenericTable, GenericTable)] = for {
      tableName <- genName
      keepCol <- genColumn
      removeCol <- genColumn.map(c => c.copy(name = s"remove_${c.name}"))
    } yield {
      val tableBefore = SchemaTable(
        tableName,
        Seq(keepCol, removeCol),
        None,
        Seq.empty,
        Seq.empty,
        Seq.empty,
        Seq.empty,
        NoTableOptions
      )
      val tableAfter = tableBefore.copy(columns = Seq(keepCol))
      (tableBefore, tableAfter)
    }

    forAll(genTablePairWithColumnRemoval, minSuccessful(20)) { case (tableBefore, tableAfter) =>
      val diffs = SchemaDiffer.diff(Seq(tableBefore), Seq(tableAfter))

      // Should produce AlterTable with DropColumn, NOT DropTable + CreateTable
      diffs.count(_.isInstanceOf[DropTable[?]]) shouldBe 0
      diffs.count(_.isInstanceOf[CreateTable[?, ?, ?]]) shouldBe 0
      diffs.count(_.isInstanceOf[AlterTable[?, ?, ?, ?]]) shouldBe 1

      val alterTable = diffs.head.asInstanceOf[AlterTable[?, ?, ?, ?]]
      alterTable.actions.size shouldBe 1
      alterTable.actions.head shouldBe a[DropColumn[?, ?]]
    }
  }
}
