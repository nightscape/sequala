package sequala.migrate.oracle

import com.dimafeng.testcontainers.OracleContainer
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import org.testcontainers.utility.DockerImageName
import sequala.migrate.SchemaDifferIntegrationSpec
import sequala.migrate.inspect.OracleInspector
import sequala.schema.*
import sequala.schema.oracle.OracleSqlRenderer.given
import sequala.schema.SqlRenderer.{toSql, given}

import java.sql.Connection

class OracleSchemaDifferIntegrationSpec extends SchemaDifferIntegrationSpec with TestContainerForAll {

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

  override def renderCreateIndex(ci: CreateIndex): String =
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
  import sequala.schema.{Table => SchemaTable}

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
}
