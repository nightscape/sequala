package sequala.migrate.postgres

import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.utility.DockerImageName
import sequala.schema.*
import sequala.schema.postgres.*

import java.sql.Connection

class PostgresSchemaInspectorSpec extends AnyFlatSpec with Matchers with TestContainerForAll {

  override val containerDef: PostgreSQLContainer.Def =
    PostgreSQLContainer.Def(DockerImageName.parse("postgres:16-alpine"))

  def executeSQL(conn: Connection, sql: String): Unit = {
    val stmt = conn.createStatement()
    stmt.execute(sql)
    stmt.close()
  }

  def dropAllTables(conn: Connection): Unit = {
    val tables = PostgresSchemaInspector.inspectTables(conn, "public")
    tables.foreach { t =>
      executeSQL(conn, s"""DROP TABLE IF EXISTS "${t.name}" CASCADE""")
    }
  }

  "PostgresSchemaInspector" should "inspect basic table with common types" in {
    withContainers { postgres =>
      Class.forName("org.postgresql.Driver")
      val conn = java.sql.DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password)
      try {
        dropAllTables(conn)
        executeSQL(
          conn,
          """
          CREATE TABLE basic_types (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            description TEXT,
            price DECIMAL(10, 2),
            quantity SMALLINT DEFAULT 0,
            is_active BOOLEAN DEFAULT true,
            created_at TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE
          )
        """
        )

        val tables = PostgresSchemaInspector.inspectTables(conn, "public")
        tables.size shouldBe 1

        val table = tables.head
        table.name shouldBe "basic_types"
        table.columns.size shouldBe 8

        val idCol = table.columns.find(_.name == "id").get
        idCol.dataType shouldBe SqlInteger
        idCol.nullable shouldBe false

        val nameCol = table.columns.find(_.name == "name").get
        nameCol.dataType shouldBe VarChar(100)
        nameCol.nullable shouldBe false

        val descCol = table.columns.find(_.name == "description").get
        descCol.dataType shouldBe SqlText
        descCol.nullable shouldBe true

        val priceCol = table.columns.find(_.name == "price").get
        priceCol.dataType shouldBe Decimal(10, 2)

        val qtyCol = table.columns.find(_.name == "quantity").get
        qtyCol.dataType shouldBe SmallInt

        val activeCol = table.columns.find(_.name == "is_active").get
        activeCol.dataType shouldBe SqlBoolean

        val createdCol = table.columns.find(_.name == "created_at").get
        createdCol.dataType shouldBe a[SqlTimestamp]
        createdCol.dataType.asInstanceOf[SqlTimestamp].withTimeZone shouldBe false

        val updatedCol = table.columns.find(_.name == "updated_at").get
        updatedCol.dataType shouldBe a[SqlTimestamp]
        updatedCol.dataType.asInstanceOf[SqlTimestamp].withTimeZone shouldBe true

        table.primaryKey shouldBe defined
        table.primaryKey.get.columns shouldBe Seq("id")
      } finally conn.close()
    }
  }

  it should "inspect PostgreSQL-specific types" in {
    withContainers { postgres =>
      val conn = java.sql.DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password)
      try {
        dropAllTables(conn)
        executeSQL(
          conn,
          """
          CREATE TABLE pg_types (
            id UUID PRIMARY KEY,
            data JSONB,
            meta JSON,
            ip_address INET,
            network CIDR,
            mac MACADDR,
            raw_data BYTEA,
            search_vector TSVECTOR,
            search_query TSQUERY,
            location POINT,
            price MONEY
          )
        """
        )

        val tables = PostgresSchemaInspector.inspectTables(conn, "public")
        val table = tables.find(_.name == "pg_types").get

        table.columns.find(_.name == "id").get.dataType shouldBe Uuid
        table.columns.find(_.name == "data").get.dataType shouldBe Jsonb
        table.columns.find(_.name == "meta").get.dataType shouldBe Json
        table.columns.find(_.name == "ip_address").get.dataType shouldBe Inet
        table.columns.find(_.name == "network").get.dataType shouldBe Cidr
        table.columns.find(_.name == "mac").get.dataType shouldBe MacAddr
        table.columns.find(_.name == "raw_data").get.dataType shouldBe Bytea
        table.columns.find(_.name == "search_vector").get.dataType shouldBe TsVector
        table.columns.find(_.name == "search_query").get.dataType shouldBe TsQuery
        table.columns.find(_.name == "location").get.dataType shouldBe PgPoint
        table.columns.find(_.name == "price").get.dataType shouldBe Money
      } finally conn.close()
    }
  }

  it should "inspect array types" in {
    withContainers { postgres =>
      val conn = java.sql.DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password)
      try {
        dropAllTables(conn)
        executeSQL(
          conn,
          """
          CREATE TABLE array_types (
            id INTEGER PRIMARY KEY,
            tags TEXT[],
            scores INTEGER[],
            matrix DECIMAL(5,2)[]
          )
        """
        )

        val tables = PostgresSchemaInspector.inspectTables(conn, "public")
        val table = tables.find(_.name == "array_types").get

        val tagsCol = table.columns.find(_.name == "tags").get
        tagsCol.dataType shouldBe a[PgArray[?]]

        val scoresCol = table.columns.find(_.name == "scores").get
        scoresCol.dataType shouldBe a[PgArray[?]]
      } finally conn.close()
    }
  }

  it should "inspect foreign keys" in {
    withContainers { postgres =>
      val conn = java.sql.DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password)
      try {
        dropAllTables(conn)
        executeSQL(
          conn,
          """
          CREATE TABLE categories (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100) NOT NULL
          )
        """
        )
        executeSQL(
          conn,
          """
          CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            category_id INTEGER REFERENCES categories(id) ON DELETE CASCADE ON UPDATE NO ACTION,
            name VARCHAR(100)
          )
        """
        )

        val tables = PostgresSchemaInspector.inspectTables(conn, "public")
        val products = tables.find(_.name == "products").get

        products.foreignKeys.size shouldBe 1
        val fk = products.foreignKeys.head
        fk.columns shouldBe Seq("category_id")
        fk.refTable shouldBe "categories"
        fk.refColumns shouldBe Seq("id")
        fk.onDelete shouldBe Cascade
        fk.onUpdate shouldBe NoAction
      } finally conn.close()
    }
  }

  it should "inspect unique constraints" in {
    withContainers { postgres =>
      val conn = java.sql.DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password)
      try {
        dropAllTables(conn)
        executeSQL(
          conn,
          """
          CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            email VARCHAR(255) UNIQUE,
            username VARCHAR(50),
            CONSTRAINT uk_username UNIQUE (username)
          )
        """
        )

        val tables = PostgresSchemaInspector.inspectTables(conn, "public")
        val users = tables.find(_.name == "users").get

        users.uniques.size shouldBe 2
        users.uniques.flatMap(_.columns).toSet shouldBe Set("email", "username")
      } finally conn.close()
    }
  }

  it should "inspect check constraints" in {
    withContainers { postgres =>
      val conn = java.sql.DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password)
      try {
        dropAllTables(conn)
        executeSQL(
          conn,
          """
          CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            quantity INTEGER CHECK (quantity > 0),
            price DECIMAL(10,2) CHECK (price >= 0),
            CONSTRAINT chk_total CHECK (quantity * price < 1000000)
          )
        """
        )

        val tables = PostgresSchemaInspector.inspectTables(conn, "public")
        val orders = tables.find(_.name == "orders").get

        orders.checks.size shouldBe 3
        orders.checks.exists(_.name.exists(_.contains("chk_total"))) shouldBe true
      } finally conn.close()
    }
  }

  it should "inspect indexes" in {
    withContainers { postgres =>
      val conn = java.sql.DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password)
      try {
        dropAllTables(conn)
        executeSQL(
          conn,
          """
          CREATE TABLE items (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100),
            category VARCHAR(50),
            price DECIMAL(10,2)
          )
        """
        )
        executeSQL(conn, "CREATE INDEX idx_items_name ON items(name)")
        executeSQL(conn, "CREATE UNIQUE INDEX idx_items_category_price ON items(category, price DESC)")
        executeSQL(conn, "CREATE INDEX idx_items_partial ON items(price) WHERE price > 100")

        val tables = PostgresSchemaInspector.inspectTables(conn, "public")
        val items = tables.find(_.name == "items").get

        items.indexes.size shouldBe 3

        val nameIdx = items.indexes.find(_.name.contains("idx_items_name")).get
        nameIdx.unique shouldBe false
        nameIdx.columns.map(_.name) shouldBe Seq("name")

        val catPriceIdx = items.indexes.find(_.name.contains("idx_items_category_price")).get
        catPriceIdx.unique shouldBe true
        catPriceIdx.columns.map(_.name) shouldBe Seq("category", "price")
        catPriceIdx.columns.find(_.name == "price").get.descending shouldBe true

        val partialIdx = items.indexes.find(_.name.contains("idx_items_partial")).get
        partialIdx.where shouldBe defined
      } finally conn.close()
    }
  }

  it should "inspect table with composite primary key" in {
    withContainers { postgres =>
      val conn = java.sql.DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password)
      try {
        dropAllTables(conn)
        executeSQL(
          conn,
          """
          CREATE TABLE order_items (
            order_id INTEGER,
            product_id INTEGER,
            quantity INTEGER NOT NULL,
            PRIMARY KEY (order_id, product_id)
          )
        """
        )

        val tables = PostgresSchemaInspector.inspectTables(conn, "public")
        val orderItems = tables.find(_.name == "order_items").get

        orderItems.primaryKey shouldBe defined
        orderItems.primaryKey.get.columns shouldBe Seq("order_id", "product_id")
      } finally conn.close()
    }
  }

  it should "inspect table inheriting from another" in {
    withContainers { postgres =>
      val conn = java.sql.DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password)
      try {
        dropAllTables(conn)
        executeSQL(
          conn,
          """
          CREATE TABLE base_entity (
            id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          )
        """
        )
        executeSQL(
          conn,
          """
          CREATE TABLE derived_entity (
            name VARCHAR(100)
          ) INHERITS (base_entity)
        """
        )

        val tables = PostgresSchemaInspector.inspectTables(conn, "public")
        val derived = tables.find(_.name == "derived_entity").get

        derived.options.inherits shouldBe Seq("base_entity")
        derived.columns.map(_.name) should contain allOf ("id", "created_at", "name")
      } finally conn.close()
    }
  }

  it should "inspect partitioned table" in {
    withContainers { postgres =>
      val conn = java.sql.DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password)
      try {
        dropAllTables(conn)
        executeSQL(
          conn,
          """
          CREATE TABLE events (
            id INTEGER,
            event_date DATE,
            data TEXT
          ) PARTITION BY RANGE (event_date)
        """
        )

        val tables = PostgresSchemaInspector.inspectTables(conn, "public")
        val events = tables.find(_.name == "events").get

        events.options.partitionBy shouldBe defined
        events.options.partitionBy.get.strategy shouldBe PostgresPartitionByRange
        events.options.partitionBy.get.columns shouldBe Seq("event_date")
      } finally conn.close()
    }
  }

  // =========================================================================
  // Schema field population tests
  // The inspector should populate the Table.schema field with the schema name
  // =========================================================================

  it should "populate schema field when inspecting tables" in {
    withContainers { postgres =>
      val conn = java.sql.DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password)
      try {
        dropAllTables(conn)
        executeSQL(conn, "CREATE TABLE test_schema_field (id INTEGER)")

        val tables = PostgresSchemaInspector.inspectTables(conn, "public")
        tables.size shouldBe 1

        val table = tables.head
        table.name shouldBe "test_schema_field"
        // The schema field should be populated with the schema we queried
        table.schema shouldBe Some("public")
        table.qualifiedName shouldBe "public.test_schema_field"
      } finally conn.close()
    }
  }

  it should "correctly set schema field for tables in non-public schemas" in {
    withContainers { postgres =>
      val conn = java.sql.DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password)
      try {
        // Create a custom schema and table
        executeSQL(conn, "CREATE SCHEMA IF NOT EXISTS myschema")
        executeSQL(conn, "DROP TABLE IF EXISTS myschema.test_table")
        executeSQL(conn, "CREATE TABLE myschema.test_table (id INTEGER)")

        val tables = PostgresSchemaInspector.inspectTables(conn, "myschema")
        tables.size shouldBe 1

        val table = tables.head
        table.name shouldBe "test_table"
        // The schema field should be "myschema"
        table.schema shouldBe Some("myschema")
        table.qualifiedName shouldBe "myschema.test_table"

        // Cleanup
        executeSQL(conn, "DROP TABLE myschema.test_table")
        executeSQL(conn, "DROP SCHEMA myschema")
      } finally conn.close()
    }
  }
}
