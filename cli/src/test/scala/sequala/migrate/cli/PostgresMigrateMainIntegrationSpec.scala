package sequala.migrate.cli

import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import org.testcontainers.utility.DockerImageName
import sequala.migrate.SchemaDifferIntegrationSpec
import sequala.migrate.inspect.PostgresInspector
import sequala.schema.*
import sequala.schema.postgres.PostgresDialect

import java.sql.Connection

class PostgresMigrateMainIntegrationSpec
    extends SchemaDifferIntegrationSpec
    with TestContainerForAll
    with PostgresDialect {

  override val containerDef: PostgreSQLContainer.Def =
    PostgreSQLContainer.Def(DockerImageName.parse("postgres:16-alpine"))

  override def withConnection[T](f: Connection => T): T = withContainers { postgres =>
    Class.forName("org.postgresql.Driver")
    val conn = java.sql.DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password)
    try f(conn)
    finally conn.close()
  }

  override def inspectTables(conn: Connection): Seq[GenericTable] =
    PostgresInspector.inspectTables(conn, "public")

  override def dropAllTables(conn: Connection): Unit = {
    val tables = PostgresInspector.inspectTables(conn, "public")
    tables.foreach { t =>
      executeSQL(conn, s"""DROP TABLE IF EXISTS "${t.name}" CASCADE""")
    }
  }

  override def defaultIndexOptions: IndexOptions = CommonIndexOptions()
}
