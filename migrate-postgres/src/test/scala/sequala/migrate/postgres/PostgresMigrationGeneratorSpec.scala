package sequala.migrate.postgres

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import sequala.migrate.{MigrationStep, SchemaDiffer}
import sequala.schema.*
import sequala.schema.postgres.*
import sequala.schema.SqlRenderer.{toSql, given}
import sequala.schema.postgres.PostgresSqlRenderer.given

class PostgresMigrationGeneratorSpec extends AnyFlatSpec with Matchers {

  def makeTable(
    name: String,
    columns: Seq[PostgresColumn],
    primaryKey: Option[PrimaryKey] = None,
    foreignKeys: Seq[ForeignKey] = Seq.empty,
    uniques: Seq[Unique] = Seq.empty,
    checks: Seq[Check] = Seq.empty
  ): PostgresTable =
    Table(name, columns, primaryKey, Seq.empty, foreignKeys, checks, uniques, PostgresTableOptions.empty)

  def makeColumn(name: String, dataType: PostgresDataType, nullable: Boolean = true): PostgresColumn =
    Column(name, dataType, nullable, None, PostgresColumnOptions.empty)

  "PostgresMigrationGenerator" should "generate CreateTable with DropTable reverse" in {
    val table = makeTable(
      "users",
      Seq(makeColumn("id", SqlInteger, nullable = false), makeColumn("name", VarChar(100))),
      primaryKey = Some(PrimaryKey(Some("pk_users"), Seq("id")))
    )

    val from = Seq.empty[PostgresTable]
    val to = Seq(table)

    val diffs = SchemaDiffer.diff(from, to)
    diffs.size shouldBe 1
    diffs.head shouldBe a[CreateTable[?, ?, ?]]

    val steps = PostgresMigrationGenerator.generate(diffs)
    steps.size shouldBe 1

    val step = steps.head
    step.statement shouldBe a[CreateTable[?, ?, ?]]
    step.reverse shouldBe defined
    step.reverse.get shouldBe a[DropTable[?]]
    step.reverse.get.asInstanceOf[DropTable[?]].tableName shouldBe "users"
    step.comment shouldBe "Create table users"
  }

  it should "generate DropTable with no reverse" in {
    val table = makeTable("users", Seq(makeColumn("id", SqlInteger)))

    val from = Seq(table)
    val to = Seq.empty[PostgresTable]

    val diffs = SchemaDiffer.diff(from, to)
    diffs.size shouldBe 1
    diffs.head shouldBe a[DropTable[?]]

    val steps = PostgresMigrationGenerator.generate(diffs)
    steps.size shouldBe 1

    val step = steps.head
    step.statement shouldBe a[DropTable[?]]
    step.reverse shouldBe empty
    step.comment shouldBe "Drop table users"
  }

  it should "generate AddColumn with DropColumn reverse" in {
    val tableBefore = makeTable("users", Seq(makeColumn("id", SqlInteger)))
    val tableAfter = makeTable("users", Seq(makeColumn("id", SqlInteger), makeColumn("email", VarChar(255))))

    val diffs = SchemaDiffer.diff(Seq(tableBefore), Seq(tableAfter))
    diffs.size shouldBe 1
    diffs.head shouldBe a[AlterTable[?, ?, ?, ?]]

    val steps = PostgresMigrationGenerator.generate(diffs)
    steps.size shouldBe 1

    val step = steps.head
    step.statement shouldBe a[AlterTable[?, ?, ?, ?]]
    step.reverse shouldBe defined
    step.reverse.get shouldBe a[AlterTable[?, ?, ?, ?]]

    val alterTable = step.statement.asInstanceOf[AlterPostgresTable]
    alterTable.actions.size shouldBe 1
    alterTable.actions.head shouldBe a[AddColumn[?, ?]]

    val reverseAlter = step.reverse.get.asInstanceOf[AlterPostgresTable]
    reverseAlter.actions.size shouldBe 1
    reverseAlter.actions.head shouldBe a[DropColumn[?, ?]]
    reverseAlter.actions.head.asInstanceOf[DropColumn[?, ?]].columnName shouldBe "email"
  }

  it should "generate DropColumn with no reverse" in {
    val tableBefore = makeTable("users", Seq(makeColumn("id", SqlInteger), makeColumn("email", VarChar(255))))
    val tableAfter = makeTable("users", Seq(makeColumn("id", SqlInteger)))

    val diffs = SchemaDiffer.diff(Seq(tableBefore), Seq(tableAfter))
    val steps = PostgresMigrationGenerator.generate(diffs)

    steps.size shouldBe 1
    val step = steps.head
    step.reverse shouldBe empty
  }

  it should "generate RenameColumn with reverse" in {
    val tableBefore = makeTable("users", Seq(makeColumn("id", SqlInteger), makeColumn("name", VarChar(100))))
    val tableAfter = makeTable("users", Seq(makeColumn("id", SqlInteger), makeColumn("full_name", VarChar(100))))

    val diffs = SchemaDiffer.diff(Seq(tableBefore), Seq(tableAfter))
    val steps = PostgresMigrationGenerator.generate(diffs)

    steps.size shouldBe 1
    val step = steps.head
    val alterTable = step.statement.asInstanceOf[AlterPostgresTable]
    alterTable.actions.exists(_.isInstanceOf[DropColumn[?, ?]]) shouldBe true
    alterTable.actions.exists(_.isInstanceOf[AddColumn[?, ?]]) shouldBe true
  }

  it should "generate AddConstraint with DropConstraint reverse for named constraints" in {
    val tableBefore = makeTable("users", Seq(makeColumn("id", SqlInteger), makeColumn("email", VarChar(255))))
    val tableAfter = makeTable(
      "users",
      Seq(makeColumn("id", SqlInteger), makeColumn("email", VarChar(255))),
      uniques = Seq(Unique(Some("uk_email"), Seq("email")))
    )

    val diffs = SchemaDiffer.diff(Seq(tableBefore), Seq(tableAfter))
    val steps = PostgresMigrationGenerator.generate(diffs)

    steps.size shouldBe 1
    val step = steps.head
    step.reverse shouldBe defined

    val alterTable = step.statement.asInstanceOf[AlterPostgresTable]
    alterTable.actions.head shouldBe a[AddConstraint[?, ?]]

    val reverseAlter = step.reverse.get.asInstanceOf[AlterPostgresTable]
    reverseAlter.actions.head shouldBe a[DropConstraint[?, ?]]
    reverseAlter.actions.head.asInstanceOf[DropConstraint[?, ?]].constraintName shouldBe "uk_email"
  }

  it should "generate multiple changes in correct order" in {
    val tableBefore = makeTable("users", Seq(makeColumn("id", SqlInteger)))
    val tableAfter = makeTable(
      "users",
      Seq(makeColumn("id", SqlInteger), makeColumn("email", VarChar(255)), makeColumn("name", VarChar(100)))
    )

    val diffs = SchemaDiffer.diff(Seq(tableBefore), Seq(tableAfter))
    val steps = PostgresMigrationGenerator.generate(diffs)

    steps.size shouldBe 1
    val alterTable = steps.head.statement.asInstanceOf[AlterPostgresTable]
    alterTable.actions.size shouldBe 2
    alterTable.actions.forall(_.isInstanceOf[AddColumn[?, ?]]) shouldBe true
  }

  it should "generate steps for CreateIndex with DropIndex reverse" in {
    val createIndexDiff: PostgresSchemaDiff =
      CreateIndex("idx_email", "users", Seq(IndexColumn("email")), unique = false)

    val steps = PostgresMigrationGenerator.generate(Seq(createIndexDiff))
    steps.size shouldBe 1

    val step = steps.head
    step.statement shouldBe a[CreateIndex[?]]
    step.reverse shouldBe defined
    step.reverse.get shouldBe a[DropIndex]
    step.reverse.get.asInstanceOf[DropIndex].name shouldBe "idx_email"
  }

  it should "render SQL via SqlRenderer" in {
    import PostgresSqlRenderer.given
    import SqlRenderer.toSql

    val table = makeTable(
      "users",
      Seq(makeColumn("id", SqlInteger, nullable = false), makeColumn("email", VarChar(255))),
      primaryKey = Some(PrimaryKey(Some("pk_users"), Seq("id")))
    )

    val createTable: PostgresSchemaDiff = CreateTable(table)
    val steps = PostgresMigrationGenerator.generate(Seq(createTable))
    val step = steps.head

    val sql = step.statement match
      case ct: CreatePostgresTable => ct.toSql
      case _ => fail("Expected CreateTable")

    sql should include("CREATE TABLE")
    sql should include("users")
    sql should include("id INTEGER")
    sql should include("email VARCHAR(255)")
    sql should include("PRIMARY KEY")
  }
}
