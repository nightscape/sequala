package sequala.migrate

import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import sequala.schema.{Table => SchemaTable, *}
import sequala.schema.GenericSqlRenderer.given
import sequala.schema.SqlRenderer.{toSql, given}

import java.sql.Connection

abstract class SchemaDifferIntegrationSpec extends AnyFlatSpec with Matchers with ScalaCheckPropertyChecks {

  def withConnection[T](f: Connection => T): T
  def inspectTables(conn: Connection): Seq[GenericTable]
  def dropAllTables(conn: Connection): Unit

  def renderCreateTable(table: GenericTable): String = CreateTable(table).toSql

  def renderAlterTable(tableName: String, action: GenericAlterTableAction): String = {
    val at = AlterTable[CommonDataType, NoColumnOptions.type, NoTableOptions.type, GenericAlterTableAction](
      tableName,
      Seq(action)
    )
    at.toSql
  }

  def renderDropTable(tableName: String): String = DropTable(tableName).toSql

  def renderCreateIndex(ci: CreateIndex): String = ci.toSql

  def renderDropIndex(di: DropIndex): String = di.toSql

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

  def genColumn: Gen[GenericColumn] = for {
    name <- genName
    dataType <- genColumnDataType
    nullable <- Gen.oneOf(true, false)
  } yield Column(name, dataType, nullable, None, NoColumnOptions)

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

  def applyDiff(
    conn: Connection,
    diffs: Seq[SchemaDiff[CommonDataType, NoColumnOptions.type, NoTableOptions.type]]
  ): Unit = {
    diffs.foreach {
      case ct: CreateTable[?, ?, ?] =>
        val sql = renderCreateTable(ct.table.asInstanceOf[GenericTable])
        executeSQL(conn, sql)
      case dt: DropTable =>
        val sql = renderDropTable(dt.tableName)
        executeSQL(conn, sql)
      case at: AlterTable[?, ?, ?, ?] =>
        at.actions.foreach { action =>
          val sql = renderAlterTable(at.tableName, action.asInstanceOf[GenericAlterTableAction])
          executeSQL(conn, sql)
        }
      case ci: CreateIndex =>
        val sql = renderCreateIndex(ci)
        executeSQL(conn, sql)
      case di: DropIndex =>
        val sql = renderDropIndex(di)
        executeSQL(conn, sql)
    }
  }

  protected def applyDesiredSchema(
    conn: Connection,
    currentTables: Seq[GenericTable],
    desiredTables: Seq[GenericTable]
  ): Unit = {
    val diffs = SchemaDiffer.diff(currentTables, desiredTables)
    applyDiff(conn, diffs)
  }

  protected def generateTablesDDL(tables: Seq[GenericTable]): String = {
    if tables.isEmpty then ";"
    else {
      val tablesDDL = tables.map(t => renderCreateTable(t))
      val indexesDDL = tables.flatMap { t =>
        t.indexes.flatMap(_.name).map { indexName =>
          val idx = t.indexes.find(_.name.contains(indexName)).get
          renderCreateIndex(CreateIndex(indexName, t.name, idx.columns, idx.unique))
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
            executeSQL(conn, renderCreateIndex(CreateIndex(name, startTable.name, idx.columns, idx.unique)))
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
            executeSQL(conn, renderCreateIndex(CreateIndex(name, startTable.name, idx.columns, idx.unique)))
          }
        }

        applyDesiredSchema(conn, Seq(startTable), Seq(desiredTable))

        val inspected = inspectTables(conn)
        inspected.size shouldBe 1
        tablesWithIndexesEquivalent(inspected.head, desiredTable) shouldBe true
      }
    }
  }
}
