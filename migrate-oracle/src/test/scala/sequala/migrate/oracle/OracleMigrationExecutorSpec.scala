package sequala.migrate.oracle

import com.dimafeng.testcontainers.OracleContainer
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.utility.DockerImageName
import sequala.migrate.*
import sequala.migrate.inspect.OracleInspector
import sequala.schema.*
import sequala.schema.oracle.*
import sequala.schema.oracle.OracleSqlRenderer.given
import sequala.schema.SqlRenderer.{toSql, given}

import java.sql.Connection
import scala.collection.mutable

class OracleMigrationExecutorSpec extends AnyFlatSpec with Matchers with TestContainerForAll {

  override val containerDef: OracleContainer.Def =
    OracleContainer.Def(DockerImageName.parse("gvenzl/oracle-xe:21-slim-faststart"))

  def withConnection[T](f: Connection => T): T = withContainers { oracle =>
    Class.forName("oracle.jdbc.OracleDriver")
    val conn = java.sql.DriverManager.getConnection(oracle.jdbcUrl, oracle.username, oracle.password)
    try f(conn)
    finally conn.close()
  }

  def dropAllTables(conn: Connection): Unit = {
    val user = conn.getMetaData.getUserName
    val tables = OracleInspector.inspectTables(conn, user)
    tables.foreach { t =>
      val stmt = conn.createStatement()
      stmt.execute(s"""DROP TABLE "${t.name}" CASCADE CONSTRAINTS""")
      stmt.close()
    }
  }

  val testTable: GenericTable = Table(
    name = "TEST_USERS",
    columns = Seq(
      Column("ID", SqlInteger, nullable = false, None, NoColumnOptions),
      Column("NAME", VarChar(100), nullable = true, Some("'anonymous'"), NoColumnOptions)
    ),
    primaryKey = None,
    indexes = Seq.empty,
    foreignKeys = Seq.empty,
    checks = Seq.empty,
    uniques = Seq.empty,
    options = NoTableOptions
  )

  "OracleMigrationExecutor" should "generate correct SQL in dry run mode" in {
    val createStep = MigrationStep[CommonDataType, NoColumnOptions.type, NoTableOptions.type](
      statement = CreateTable(testTable),
      reverse = Some(DropTable("TEST_USERS")),
      comment = "Create TEST_USERS table"
    )

    val result = OracleMigrationExecutor.executeDryRun(Seq(createStep))

    result.statements.size shouldBe 1
    result.statements.head.sql should include("CREATE TABLE")
    result.statements.head.sql should include(""""TEST_USERS"""")
    result.statements.head.sql should include("NUMBER(10)")
    result.statements.head.comment shouldBe "Create TEST_USERS table"
    // Oracle requires quoted identifiers to preserve case
    result.statements.head.reverseSql shouldBe Some("""DROP TABLE "TEST_USERS"""")
  }

  it should "not support transactional DDL (should use auto-commit)" in {
    OracleMigrationExecutor.supportsTransactionalDDL shouldBe false
  }

  it should "execute migrations successfully" in withConnection { conn =>
    dropAllTables(conn)

    val createStep = MigrationStep[CommonDataType, NoColumnOptions.type, NoTableOptions.type](
      statement = CreateTable(testTable),
      reverse = Some(DropTable("TEST_USERS")),
      comment = "Create TEST_USERS table"
    )

    val result = OracleMigrationExecutor.execute(conn, Seq(createStep), mode = TransactionMode.Transactional)

    result.successful shouldBe true
    result.executedCount shouldBe 1

    val user = conn.getMetaData.getUserName
    val tables = OracleInspector.inspectTables(conn, user)
    tables.map(_.name) should contain("TEST_USERS")

    // Also verify OracleSchemaInspector works (exercises LONG column handling)
    val oracleTables = OracleSchemaInspector.inspectTables(conn, user)
    oracleTables.map(_.name) should contain("TEST_USERS")
  }

  it should "call callbacks during execution" in withConnection { conn =>
    dropAllTables(conn)

    val createStep = MigrationStep[CommonDataType, NoColumnOptions.type, NoTableOptions.type](
      statement = CreateTable(testTable),
      reverse = Some(DropTable("TEST_USERS")),
      comment = "Create TEST_USERS table"
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

    OracleMigrationExecutor.execute(conn, Seq(createStep), callback = callback)

    events should contain("starting:0")
    events should contain("completed:0")
  }

  it should "execute multiple migration steps" in withConnection { conn =>
    dropAllTables(conn)

    val createStep = MigrationStep[CommonDataType, NoColumnOptions.type, NoTableOptions.type](
      statement = CreateTable(testTable),
      reverse = Some(DropTable("TEST_USERS")),
      comment = "Create TEST_USERS table"
    )

    val addColumnStep = MigrationStep[CommonDataType, NoColumnOptions.type, NoTableOptions.type](
      statement = AlterTable[CommonDataType, NoColumnOptions.type, NoTableOptions.type, GenericAlterTableAction](
        "TEST_USERS",
        Seq(AddColumn(Column("EMAIL", VarChar(255), nullable = true, None, NoColumnOptions)))
      ),
      reverse = Some(
        AlterTable[CommonDataType, NoColumnOptions.type, NoTableOptions.type, GenericAlterTableAction](
          "TEST_USERS",
          Seq(DropColumn("EMAIL"))
        )
      ),
      comment = "Add EMAIL column"
    )

    val result = OracleMigrationExecutor.execute(conn, Seq(createStep, addColumnStep))

    result.successful shouldBe true
    result.executedCount shouldBe 2

    val user = conn.getMetaData.getUserName
    val tables = OracleInspector.inspectTables(conn, user)
    val usersTable = tables.find(_.name == "TEST_USERS")
    usersTable shouldBe defined
    usersTable.get.columns.map(_.name) should contain("EMAIL")
  }

  it should "attempt manual rollback on failure" in withConnection { conn =>
    dropAllTables(conn)

    val createStep1 = MigrationStep[CommonDataType, NoColumnOptions.type, NoTableOptions.type](
      statement = CreateTable(testTable),
      reverse = Some(DropTable("TEST_USERS")),
      comment = "Create TEST_USERS table"
    )

    val invalidStep = MigrationStep[CommonDataType, NoColumnOptions.type, NoTableOptions.type](
      statement = CreateTable(
        Table(
          name = "INVALID_TABLE",
          columns = Seq(Column("ID", SqlInteger, nullable = false, None, NoColumnOptions)),
          primaryKey = None,
          indexes = Seq.empty,
          foreignKeys =
            Seq(ForeignKey(Some("FK_INVALID"), Seq("NONEXISTENT"), "NONEXISTENT_TABLE", Seq("ID"), NoAction, NoAction)),
          checks = Seq.empty,
          uniques = Seq.empty,
          options = NoTableOptions
        )
      ),
      reverse = Some(DropTable("INVALID_TABLE")),
      comment = "Create invalid table"
    )

    val rollbackEvents = mutable.Buffer[String]()
    val callback = new ExecutionCallback {
      override def onRollbackStarting(stepIndex: Int, reverseSql: String): Unit =
        rollbackEvents += s"rollback:$stepIndex"
    }

    val result = OracleMigrationExecutor.execute(
      conn,
      Seq(createStep1, invalidStep),
      rollbackOnFailure = true,
      callback = callback
    )

    result.successful shouldBe false
    rollbackEvents should contain("rollback:0")

    val user = conn.getMetaData.getUserName
    val tables = OracleInspector.inspectTables(conn, user)
    tables.map(_.name) should not contain "TEST_USERS"
  }
}
