package sequala.migrate.oracle

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import sequala.migrate.{MigrationStep, SchemaDiffer}
import sequala.schema.*
import sequala.schema.oracle.*
import sequala.schema.SqlRenderer.{toSql, given}
import sequala.schema.oracle.OracleSqlRenderer.given

class OracleMigrationGeneratorSpec extends AnyFlatSpec with Matchers {

  def makeTable(
    name: String,
    columns: Seq[OracleColumn],
    primaryKey: Option[PrimaryKey] = None,
    foreignKeys: Seq[ForeignKey] = Seq.empty,
    uniques: Seq[Unique] = Seq.empty,
    checks: Seq[Check] = Seq.empty
  ): OracleTable =
    Table(name, columns, primaryKey, Seq.empty, foreignKeys, checks, uniques, OracleTableOptions.empty)

  def makeColumn(name: String, dataType: OracleDataType, nullable: Boolean = true): OracleColumn =
    Column(name, dataType, nullable, None, OracleColumnOptions.empty)

  "OracleMigrationGenerator" should "generate CreateTable with DropTable reverse" in {
    val table = makeTable(
      "users",
      Seq(makeColumn("id", Number(Some(10), None), nullable = false), makeColumn("name", Varchar2(100))),
      primaryKey = Some(PrimaryKey(Some("pk_users"), Seq("id")))
    )

    val from = Seq.empty[OracleTable]
    val to = Seq(table)

    val diffs = SchemaDiffer.diff(from, to)
    diffs.size shouldBe 1
    diffs.head shouldBe a[CreateTable[?, ?, ?]]

    val steps = OracleMigrationGenerator.generate(diffs)
    steps.size shouldBe 1

    val step = steps.head
    step.statement shouldBe a[CreateTable[?, ?, ?]]
    step.reverse shouldBe defined
    step.reverse.get shouldBe a[DropTable[?]]
    step.reverse.get.asInstanceOf[DropTable[?]].tableName shouldBe "users"
    step.comment shouldBe "Create table users"
  }

  it should "generate DropTable with no reverse" in {
    val table = makeTable("users", Seq(makeColumn("id", Number(Some(10), None))))

    val from = Seq(table)
    val to = Seq.empty[OracleTable]

    val diffs = SchemaDiffer.diff(from, to)
    val steps = OracleMigrationGenerator.generate(diffs)

    steps.size shouldBe 1
    val step = steps.head
    step.statement shouldBe a[DropTable[?]]
    step.reverse shouldBe empty
  }

  it should "generate AddColumn with DropColumn reverse" in {
    val tableBefore = makeTable("users", Seq(makeColumn("id", Number(Some(10), None))))
    val tableAfter =
      makeTable("users", Seq(makeColumn("id", Number(Some(10), None)), makeColumn("email", Varchar2(255))))

    val diffs = SchemaDiffer.diff(Seq(tableBefore), Seq(tableAfter))
    val steps = OracleMigrationGenerator.generate(diffs)

    steps.size shouldBe 1
    val step = steps.head

    step.reverse shouldBe defined
    val alterTable = step.statement.asInstanceOf[AlterOracleTable]
    alterTable.actions.head shouldBe a[AddColumn[?, ?]]

    val reverseAlter = step.reverse.get.asInstanceOf[AlterOracleTable]
    reverseAlter.actions.head shouldBe a[DropColumn[?, ?]]
  }

  it should "render SQL via SqlRenderer" in {
    import OracleSqlRenderer.given
    import SqlRenderer.toSql

    val table = makeTable(
      "users",
      Seq(makeColumn("id", Number(Some(10), None), nullable = false), makeColumn("email", Varchar2(255))),
      primaryKey = Some(PrimaryKey(Some("pk_users"), Seq("id")))
    )

    val createTable: OracleSchemaDiff = CreateTable(table)
    val steps = OracleMigrationGenerator.generate(Seq(createTable))
    val step = steps.head

    val sql = step.statement match
      case ct: CreateOracleTable => ct.toSql
      case _ => fail("Expected CreateTable")

    sql should include("CREATE TABLE")
    sql should include("users")
    sql should include("NUMBER(10)")
    sql should include("VARCHAR2(255 BYTE)")
    sql should include("PRIMARY KEY")
  }
}
