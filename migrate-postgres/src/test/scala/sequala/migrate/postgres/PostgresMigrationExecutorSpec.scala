package sequala.migrate.postgres

import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.utility.DockerImageName
import sequala.migrate.*
import sequala.migrate.inspect.PostgresInspector
import sequala.schema.*
import sequala.schema.postgres.*
import sequala.schema.postgres.PostgresSqlRenderer.given
import sequala.schema.SqlRenderer.{toSql, given}

import java.sql.Connection
import scala.collection.mutable

class PostgresMigrationExecutorSpec extends AnyFlatSpec with Matchers with TestContainerForAll {

  override val containerDef: PostgreSQLContainer.Def =
    PostgreSQLContainer.Def(DockerImageName.parse("postgres:16-alpine"))

  def withConnection[T](f: Connection => T): T = withContainers { postgres =>
    Class.forName("org.postgresql.Driver")
    val conn = java.sql.DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password)
    try f(conn)
    finally conn.close()
  }

  def dropAllTables(conn: Connection): Unit = {
    val tables = PostgresInspector.inspectTables(conn, "public")
    tables.foreach { t =>
      val stmt = conn.createStatement()
      stmt.execute(s"""DROP TABLE IF EXISTS "${t.name}" CASCADE""")
      stmt.close()
    }
  }

  val testTable: GenericTable = Table(
    name = "test_users",
    columns = Seq(
      Column("id", SqlInteger, nullable = false, None, NoColumnOptions),
      Column("name", VarChar(100), nullable = true, None, NoColumnOptions)
    ),
    primaryKey = None,
    indexes = Seq.empty,
    foreignKeys = Seq.empty,
    checks = Seq.empty,
    uniques = Seq.empty,
    options = NoTableOptions
  )

  "PostgresMigrationExecutor" should "generate correct SQL in dry run mode" in {
    val createStep = MigrationStep[CommonDataType, NoColumnOptions.type, NoTableOptions.type](
      statement = CreateTable(testTable),
      reverse = Some(DropTable("test_users")),
      comment = "Create test_users table"
    )

    val result = GenericMigrationExecutor.executeDryRun(Seq(createStep))

    result.statements.size shouldBe 1
    result.statements.head.sql should include("CREATE TABLE")
    result.statements.head.sql should include("test_users")
    result.statements.head.comment shouldBe "Create test_users table"
    result.statements.head.reverseSql shouldBe Some("DROP TABLE test_users")
  }

  it should "execute migrations successfully with transactional mode" in withConnection { conn =>
    dropAllTables(conn)

    val createStep = MigrationStep[CommonDataType, NoColumnOptions.type, NoTableOptions.type](
      statement = CreateTable(testTable),
      reverse = Some(DropTable("test_users")),
      comment = "Create test_users table"
    )

    val result = GenericMigrationExecutor.execute(conn, Seq(createStep), mode = TransactionMode.Transactional)

    result.successful shouldBe true
    result.executedCount shouldBe 1

    val tables = PostgresInspector.inspectTables(conn, "public")
    tables.map(_.name) should contain("test_users")
  }

  it should "rollback on failure in transactional mode" in withConnection { conn =>
    dropAllTables(conn)

    val createStep1 = MigrationStep[CommonDataType, NoColumnOptions.type, NoTableOptions.type](
      statement = CreateTable(testTable),
      reverse = Some(DropTable("test_users")),
      comment = "Create test_users table"
    )

    val invalidStep = MigrationStep[CommonDataType, NoColumnOptions.type, NoTableOptions.type](
      statement = CreateTable(
        Table(
          name = "invalid_table",
          columns = Seq(Column("id", SqlInteger, nullable = false, None, NoColumnOptions)),
          primaryKey = None,
          indexes = Seq.empty,
          foreignKeys =
            Seq(ForeignKey(Some("fk_invalid"), Seq("nonexistent"), "nonexistent_table", Seq("id"), NoAction, NoAction)),
          checks = Seq.empty,
          uniques = Seq.empty,
          options = NoTableOptions
        )
      ),
      reverse = Some(DropTable("invalid_table")),
      comment = "Create invalid table"
    )

    val result = GenericMigrationExecutor.execute(
      conn,
      Seq(createStep1, invalidStep),
      mode = TransactionMode.Transactional,
      rollbackOnFailure = true
    )

    result.successful shouldBe false
    result.steps.exists(_.status == StepStatus.RolledBack) shouldBe true

    val tables = PostgresInspector.inspectTables(conn, "public")
    tables.map(_.name) should not contain "test_users"
  }

  it should "call callbacks during execution" in withConnection { conn =>
    dropAllTables(conn)

    val createStep = MigrationStep[CommonDataType, NoColumnOptions.type, NoTableOptions.type](
      statement = CreateTable(testTable),
      reverse = Some(DropTable("test_users")),
      comment = "Create test_users table"
    )

    val events = mutable.Buffer[String]()

    val callback = new ExecutionCallback {
      override def onStepStarting(stepIndex: Int, sql: String, comment: String): Unit =
        events += s"starting:$stepIndex"

      override def onStepCompleted(stepIndex: Int, sql: String, duration: scala.concurrent.duration.Duration): Unit =
        events += s"completed:$stepIndex"

      override def onStepFailed(stepIndex: Int, sql: String, error: Throwable): Unit =
        events += s"failed:$stepIndex"
    }

    GenericMigrationExecutor.execute(conn, Seq(createStep), mode = TransactionMode.Transactional, callback = callback)

    events should contain("starting:0")
    events should contain("completed:0")
  }

  it should "execute in auto-commit mode" in withConnection { conn =>
    dropAllTables(conn)

    val createStep = MigrationStep[CommonDataType, NoColumnOptions.type, NoTableOptions.type](
      statement = CreateTable(testTable),
      reverse = Some(DropTable("test_users")),
      comment = "Create test_users table"
    )

    val result = GenericMigrationExecutor.execute(conn, Seq(createStep), mode = TransactionMode.AutoCommit)

    result.successful shouldBe true

    val tables = PostgresInspector.inspectTables(conn, "public")
    tables.map(_.name) should contain("test_users")
  }

  it should "execute multiple migration steps" in withConnection { conn =>
    dropAllTables(conn)

    val createStep = MigrationStep[CommonDataType, NoColumnOptions.type, NoTableOptions.type](
      statement = CreateTable(testTable),
      reverse = Some(DropTable("test_users")),
      comment = "Create test_users table"
    )

    val addColumnStep = MigrationStep[CommonDataType, NoColumnOptions.type, NoTableOptions.type](
      statement = AlterTable[CommonDataType, NoColumnOptions.type, NoTableOptions.type, GenericAlterTableAction](
        "test_users",
        Seq(AddColumn(Column("email", VarChar(255), nullable = true, None, NoColumnOptions)))
      ),
      reverse = Some(
        AlterTable[CommonDataType, NoColumnOptions.type, NoTableOptions.type, GenericAlterTableAction](
          "test_users",
          Seq(DropColumn("email"))
        )
      ),
      comment = "Add email column"
    )

    val result =
      GenericMigrationExecutor.execute(conn, Seq(createStep, addColumnStep), mode = TransactionMode.Transactional)

    result.successful shouldBe true
    result.executedCount shouldBe 2

    val tables = PostgresInspector.inspectTables(conn, "public")
    val usersTable = tables.find(_.name == "test_users")
    usersTable shouldBe defined
    usersTable.get.columns.map(_.name) should contain("email")
  }
}
