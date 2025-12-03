package sequala

import org.specs2.mutable.Specification
import org.scalacheck.{Gen, Prop, Properties}
import org.scalacheck.Prop.propBoolean
import sequala.ansi.ANSISQL
import sequala.schema.{
  Cascade,
  CommonDataType,
  CreateIndex,
  CreateTable as SchemaCreateTable,
  GenericSqlRenderer,
  NoAction,
  NoColumnOptions,
  NoTableOptions,
  SetNull,
  SqlRenderer
}
import sequala.schema.SqlRenderer.toSql
import sequala.common.renderer.ParserSqlRenderers.given
import sequala.converter.{AnsiSchemaBuilder, SchemaBuilder}
import sequala.schema.SqlFormatConfig

class SchemaConverterSpec extends Specification:

  given SqlFormatConfig = SqlFormatConfig.Compact

  type CreateGenericTable = SchemaCreateTable[CommonDataType, NoColumnOptions.type, NoTableOptions.type]

  "SchemaConverter" should {
    "convert simple CREATE TABLE" in {
      val sql = "CREATE TABLE users (id INTEGER, name VARCHAR(100));"
      val parsed = ANSISQL(sql)
      parsed.isSuccess must beTrue

      val stmt = parsed.get.value.asInstanceOf[CreateGenericTable]
      val schemaTable = stmt

      schemaTable.table.name mustEqual "users"
      schemaTable.table.columns.length mustEqual 2
      schemaTable.table.columns(0).name mustEqual "id"
      schemaTable.table.columns(1).name mustEqual "name"
    }

    "convert CREATE TABLE with NOT NULL constraints" in {
      val sql = "CREATE TABLE users (id INTEGER NOT NULL, name VARCHAR(100));"
      val parsed = ANSISQL(sql)
      parsed.isSuccess must beTrue

      val stmt = parsed.get.value.asInstanceOf[CreateGenericTable]
      val schemaTable = stmt

      schemaTable.table.columns(0).nullable must beFalse
      schemaTable.table.columns(1).nullable must beTrue
    }

    "convert CREATE TABLE with PRIMARY KEY on column" in {
      val sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100));"
      val parsed = ANSISQL(sql)
      parsed.isSuccess must beTrue

      val stmt = parsed.get.value.asInstanceOf[CreateGenericTable]
      val schemaTable = stmt

      schemaTable.table.primaryKey must beSome
      schemaTable.table.primaryKey.get.columns mustEqual Seq("id")
    }

    "convert CREATE TABLE with table-level PRIMARY KEY" in {
      val sql = "CREATE TABLE users (id INTEGER, name VARCHAR(100), PRIMARY KEY (id));"
      val parsed = ANSISQL(sql)
      parsed.isSuccess must beTrue

      val stmt = parsed.get.value.asInstanceOf[CreateGenericTable]
      val schemaTable = stmt

      schemaTable.table.primaryKey must beSome
      schemaTable.table.primaryKey.get.columns mustEqual Seq("id")
    }

    "convert CREATE TABLE with DEFAULT value" in {
      val sql = "CREATE TABLE users (id INTEGER, active BOOLEAN DEFAULT TRUE);"
      val parsed = ANSISQL(sql)
      parsed.isSuccess must beTrue

      val stmt = parsed.get.value.asInstanceOf[CreateGenericTable]
      val schemaTable = stmt

      schemaTable.table.columns(1).default must beSome
    }

    "generate valid SQL from schema" in {
      val sql = "CREATE TABLE users (id INTEGER NOT NULL, name VARCHAR(100));"
      val parsed = ANSISQL(sql)
      parsed.isSuccess must beTrue

      val stmt = parsed.get.value.asInstanceOf[CreateGenericTable]
      val generatedSql = stmt.toSql

      generatedSql must contain("CREATE TABLE")
      generatedSql must contain("users")
      generatedSql must contain("id")
      generatedSql must contain("INTEGER")
      generatedSql must contain("NOT NULL")
      generatedSql must contain("name")
      generatedSql must contain("VARCHAR(100)")
    }

    "round-trip: parse -> schema -> toSql -> parse preserves structure" in {
      val sql = "CREATE TABLE products (id INTEGER NOT NULL, name VARCHAR(255), price DECIMAL(10, 2));"
      val parsed = ANSISQL(sql)
      parsed.isSuccess must beTrue

      val stmt = parsed.get.value.asInstanceOf[CreateGenericTable]
      val generatedSql = stmt.toSql

      val reparsed = ANSISQL(generatedSql)
      reparsed.isSuccess must beTrue

      val restmt = reparsed.get.value.asInstanceOf[CreateGenericTable]

      restmt.table.name mustEqual stmt.table.name
      restmt.table.columns.length mustEqual stmt.table.columns.length

      restmt.table.columns.zip(stmt.table.columns).foreach { case (reparsedCol, originalCol) =>
        reparsedCol.name mustEqual originalCol.name
        reparsedCol.nullable mustEqual originalCol.nullable
      }
      ok
    }

    "convert CREATE TABLE with FOREIGN KEY" in {
      val sql = "CREATE TABLE orders (id INTEGER, user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id));"
      val parsed = ANSISQL(sql)
      parsed.isSuccess must beTrue

      val stmt = parsed.get.value.asInstanceOf[CreateGenericTable]
      val table = stmt.table

      table.foreignKeys.length mustEqual 1
      table.foreignKeys.head.columns mustEqual Seq("user_id")
      table.foreignKeys.head.refTable mustEqual "users"
      table.foreignKeys.head.refColumns mustEqual Seq("id")
    }

    "convert CREATE TABLE with named FOREIGN KEY" in {
      val sql =
        "CREATE TABLE orders (id INTEGER, user_id INTEGER, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id));"
      val parsed = ANSISQL(sql)
      parsed.isSuccess must beTrue

      val stmt = parsed.get.value.asInstanceOf[CreateGenericTable]
      val table = stmt.table

      table.foreignKeys.length mustEqual 1
      table.foreignKeys.head.name must beSome("fk_user")
      table.foreignKeys.head.columns mustEqual Seq("user_id")
      table.foreignKeys.head.refTable mustEqual "users"
      table.foreignKeys.head.refColumns mustEqual Seq("id")
    }

    "convert CREATE TABLE with FOREIGN KEY and ON DELETE CASCADE" in {
      val sql =
        "CREATE TABLE orders (id INTEGER, user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE);"
      val parsed = ANSISQL(sql)
      parsed.isSuccess must beTrue

      val stmt = parsed.get.value.asInstanceOf[CreateGenericTable]
      val fk = stmt.table.foreignKeys.head

      fk.onDelete mustEqual Cascade
      fk.onUpdate mustEqual NoAction
    }

    "convert CREATE TABLE with FOREIGN KEY and ON UPDATE SET NULL" in {
      val sql =
        "CREATE TABLE orders (id INTEGER, user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id) ON UPDATE SET NULL);"
      val parsed = ANSISQL(sql)
      parsed.isSuccess must beTrue

      val stmt = parsed.get.value.asInstanceOf[CreateGenericTable]
      val fk = stmt.table.foreignKeys.head

      fk.onUpdate mustEqual SetNull
      fk.onDelete mustEqual NoAction
    }

    "convert CREATE TABLE with FOREIGN KEY with multiple columns" in {
      val sql =
        "CREATE TABLE order_items (order_id INTEGER, product_id INTEGER, FOREIGN KEY (order_id, product_id) REFERENCES orders(id, line));"
      val parsed = ANSISQL(sql)
      parsed.isSuccess must beTrue

      val stmt = parsed.get.value.asInstanceOf[CreateGenericTable]
      val fk = stmt.table.foreignKeys.head

      fk.columns mustEqual Seq("order_id", "product_id")
      fk.refColumns mustEqual Seq("id", "line")
    }

    "convert CREATE TABLE with UNIQUE constraint" in {
      val sql = "CREATE TABLE users (id INTEGER, email VARCHAR(255), UNIQUE (email));"
      val parsed = ANSISQL(sql)
      parsed.isSuccess must beTrue

      val stmt = parsed.get.value.asInstanceOf[CreateGenericTable]
      val table = stmt.table

      table.uniques.length mustEqual 1
      table.uniques.head.columns mustEqual Seq("email")
    }

    "convert CREATE TABLE with named UNIQUE constraint" in {
      val sql = "CREATE TABLE users (id INTEGER, email VARCHAR(255), CONSTRAINT uq_email UNIQUE (email));"
      val parsed = ANSISQL(sql)
      parsed.isSuccess must beTrue

      val stmt = parsed.get.value.asInstanceOf[CreateGenericTable]
      val table = stmt.table

      table.uniques.length mustEqual 1
      table.uniques.head.columns mustEqual Seq("email")
    }

    "parse CREATE INDEX statement" in {
      val sql = "CREATE INDEX idx_users_email ON users(email);"
      val parsed = ANSISQL(sql)
      parsed.isSuccess must beTrue

      val stmt = parsed.get.value.asInstanceOf[CreateIndex[?]]
      stmt.name mustEqual "idx_users_email"
      stmt.tableName mustEqual "users"
      stmt.columns.map(_.name) mustEqual Seq("email")
      stmt.unique must beFalse
    }

    "parse CREATE UNIQUE INDEX statement" in {
      val sql = "CREATE UNIQUE INDEX idx_users_email ON users(email);"
      val parsed = ANSISQL(sql)
      parsed.isSuccess must beTrue

      val stmt = parsed.get.value.asInstanceOf[CreateIndex[?]]
      stmt.unique must beTrue
    }

    "parse CREATE INDEX with multiple columns" in {
      val sql = "CREATE INDEX idx_orders_user_date ON orders(user_id, created_at);"
      val parsed = ANSISQL(sql)
      parsed.isSuccess must beTrue

      val stmt = parsed.get.value.asInstanceOf[CreateIndex[?]]
      stmt.columns.map(_.name) mustEqual Seq("user_id", "created_at")
    }

    // =========================================================================
    // Schema-qualified table name tests
    // These tests verify that CREATE TABLE with schema-qualified names
    // correctly populates the Table.schema field
    // =========================================================================

    "parse CREATE TABLE with schema-qualified name (schema.table)" in {
      val sql = "CREATE TABLE myschema.users (id INTEGER, name VARCHAR(100));"
      val parsed = ANSISQL(sql)
      parsed.isSuccess must beTrue

      val stmt = parsed.get.value.asInstanceOf[CreateGenericTable]
      // The schema should be extracted into the schema field
      stmt.table.schema must beSome("myschema")
      stmt.table.name mustEqual "users"
      stmt.table.qualifiedName mustEqual "myschema.users"
    }

    "parse CREATE TABLE with quoted schema-qualified name" in {
      val sql = """CREATE TABLE "myschema"."users" (id INTEGER);"""
      val parsed = ANSISQL(sql)
      parsed.isSuccess must beTrue

      val stmt = parsed.get.value.asInstanceOf[CreateGenericTable]
      stmt.table.schema must beSome("myschema")
      stmt.table.name mustEqual "users"
    }

    "parse CREATE TABLE without schema should have schema=None" in {
      val sql = "CREATE TABLE users (id INTEGER);"
      val parsed = ANSISQL(sql)
      parsed.isSuccess must beTrue

      val stmt = parsed.get.value.asInstanceOf[CreateGenericTable]
      stmt.table.schema must beNone
      stmt.table.name mustEqual "users"
      stmt.table.qualifiedName mustEqual "users"
    }
  }

  "SchemaBuilder" should {
    "build tables from multiple statements" in {
      val statements = ANSISQL.parseAll("""
        CREATE TABLE users (id INTEGER, name VARCHAR(100));
        CREATE TABLE orders (id INTEGER, user_id INTEGER);
      """)

      val tables = AnsiSchemaBuilder.fromStatements(statements.flatMap(_.result.toOption))

      tables.length mustEqual 2
      tables.map(_.name).toSet mustEqual Set("users", "orders")
    }

    "merge standalone CREATE INDEX into table" in {
      val statements = ANSISQL.parseAll("""
        CREATE TABLE users (id INTEGER, email VARCHAR(255));
        CREATE INDEX idx_users_email ON users(email);
      """)

      val tables = AnsiSchemaBuilder.fromStatements(statements.flatMap(_.result.toOption))

      tables.length mustEqual 1
      val usersTable = tables.head
      usersTable.indexes.length mustEqual 1
      usersTable.indexes.head.name must beSome("idx_users_email")
      usersTable.indexes.head.columns.map(_.name) mustEqual Seq("email")
    }

    "merge multiple indexes into table" in {
      val statements = ANSISQL.parseAll("""
        CREATE TABLE users (id INTEGER, email VARCHAR(255), name VARCHAR(100));
        CREATE INDEX idx_users_email ON users(email);
        CREATE UNIQUE INDEX idx_users_name ON users(name);
      """)

      val tables = AnsiSchemaBuilder.fromStatements(statements.flatMap(_.result.toOption))

      val usersTable = tables.head
      usersTable.indexes.length mustEqual 2
      usersTable.indexes.exists(_.unique) must beTrue
    }

    "preserve inline indexes and add standalone indexes" in {
      val statements = ANSISQL.parseAll("""
        CREATE TABLE users (id INTEGER, email VARCHAR(255), name VARCHAR(100), INDEX (id));
        CREATE INDEX idx_users_email ON users(email);
      """)

      val tables = AnsiSchemaBuilder.fromStatements(statements.flatMap(_.result.toOption))

      val usersTable = tables.head
      usersTable.indexes.length mustEqual 2
    }
  }

object SchemaConverterPropertySpec extends Properties("SchemaConverter"):
  import GenericSqlRenderer.given
  import sequala.schema.SqlFormatConfig

  type CreateGenericTable = SchemaCreateTable[CommonDataType, NoColumnOptions.type, NoTableOptions.type]

  val genIdentifier: Gen[String] = for {
    first <- Gen.alphaChar
    rest <- Gen.listOfN(Gen.choose(2, 10).sample.getOrElse(5), Gen.alphaNumChar)
  } yield {
    val name = (first :: rest).mkString
    if sequala.common.parser.SQLBase.reservedWordsSet.contains(name.toUpperCase) then s"col_$name"
    else name
  }

  val genDataType: Gen[String] = Gen.oneOf(
    Gen.const("INTEGER"),
    Gen.const("BIGINT"),
    Gen.const("SMALLINT"),
    Gen.const("BOOLEAN"),
    Gen.const("TEXT"),
    Gen.const("DATE"),
    Gen.choose(1, 255).map(n => s"VARCHAR($n)"),
    Gen.choose(1, 100).map(n => s"CHAR($n)"),
    for {
      p <- Gen.choose(1, 38)
      s <- Gen.choose(0, p)
    } yield s"DECIMAL($p, $s)"
  )

  val genColumn: Gen[String] = for {
    name <- genIdentifier
    dataType <- genDataType
    notNull <- Gen.oneOf("", " NOT NULL")
  } yield s"$name $dataType$notNull"

  val genCreateTable: Gen[String] = for {
    tableName <- genIdentifier
    numCols <- Gen.choose(1, 5)
    columns <- Gen.listOfN(numCols, genColumn)
  } yield s"CREATE TABLE $tableName (${columns.mkString(", ")});"

  property("round-trip preserves table name") = Prop.forAll(genCreateTable) { sql =>
    import sequala.schema.SqlRenderer.toSql
    given sequala.schema.SqlFormatConfig = sequala.schema.SqlFormatConfig.Compact
    val parsed = ANSISQL(sql)
    if !parsed.isSuccess || !parsed.get.value.isInstanceOf[CreateGenericTable] then true
    else
      val stmt = parsed.get.value.asInstanceOf[CreateGenericTable]
      val generatedSql = stmt.toSql
      val reparsed = ANSISQL(generatedSql)
      if !reparsed.isSuccess || !reparsed.get.value.isInstanceOf[CreateGenericTable] then true
      else
        val restmt = reparsed.get.value.asInstanceOf[CreateGenericTable]
        restmt.table.name == stmt.table.name
  }

  property("round-trip preserves column count") = Prop.forAll(genCreateTable) { sql =>
    import sequala.schema.SqlRenderer.toSql
    given sequala.schema.SqlFormatConfig = sequala.schema.SqlFormatConfig.Compact
    val parsed = ANSISQL(sql)
    if !parsed.isSuccess || !parsed.get.value.isInstanceOf[CreateGenericTable] then true
    else
      val stmt = parsed.get.value.asInstanceOf[CreateGenericTable]
      val generatedSql = stmt.toSql
      val reparsed = ANSISQL(generatedSql)
      if !reparsed.isSuccess || !reparsed.get.value.isInstanceOf[CreateGenericTable] then true
      else
        val restmt = reparsed.get.value.asInstanceOf[CreateGenericTable]
        restmt.table.columns.length == stmt.table.columns.length
  }

  property("round-trip preserves nullability") = Prop.forAll(genCreateTable) { sql =>
    import sequala.schema.SqlRenderer.toSql
    given sequala.schema.SqlFormatConfig = sequala.schema.SqlFormatConfig.Compact
    val parsed = ANSISQL(sql)
    if !parsed.isSuccess || !parsed.get.value.isInstanceOf[CreateGenericTable] then true
    else
      val stmt = parsed.get.value.asInstanceOf[CreateGenericTable]
      val generatedSql = stmt.toSql
      val reparsed = ANSISQL(generatedSql)
      if !reparsed.isSuccess || !reparsed.get.value.isInstanceOf[CreateGenericTable] then true
      else
        val restmt = reparsed.get.value.asInstanceOf[CreateGenericTable]
        restmt.table.columns.map(_.nullable) == stmt.table.columns.map(_.nullable)
  }

  property("pretty format round-trip parses correctly") = Prop.forAll(genCreateTable) { sql =>
    given SqlFormatConfig = SqlFormatConfig.Pretty
    val parsed = ANSISQL(sql)
    if !parsed.isSuccess || !parsed.get.value.isInstanceOf[CreateGenericTable] then true
    else
      val stmt = parsed.get.value.asInstanceOf[CreateGenericTable]
      val prettySql = summon[SqlRenderer[SchemaCreateTable[CommonDataType, NoColumnOptions.type, NoTableOptions.type]]]
        .toSql(stmt) + ";"
      val reparsed = ANSISQL(prettySql)
      if !reparsed.isSuccess then
        println(s"Failed to parse pretty SQL:\n$prettySql")
        println(s"Error: ${reparsed}")
        false
      else if !reparsed.get.value.isInstanceOf[CreateGenericTable] then
        println(s"Parsed to wrong type: ${reparsed.get.value.getClass}")
        false
      else
        val restmt = reparsed.get.value.asInstanceOf[CreateGenericTable]
        val nameMatch = restmt.table.name == stmt.table.name
        val colCountMatch = restmt.table.columns.length == stmt.table.columns.length
        if !nameMatch || !colCountMatch then
          println(s"Pretty SQL:\n$prettySql")
          println(s"Name match: $nameMatch, Col count match: $colCountMatch")
        nameMatch && colCountMatch
  }

  // ============================================================================
  // Schema-qualified table name PBTs
  // ============================================================================

  val genSchemaQualifiedTableDDL: Gen[String] = for {
    schema <- genIdentifier
    table <- genIdentifier
    numCols <- Gen.choose(1, 3)
    columns <- Gen.listOfN(numCols, genColumn)
  } yield s"CREATE TABLE $schema.$table (${columns.mkString(", ")});"

  property("parse schema-qualified CREATE TABLE extracts schema field") = Prop.forAll(genSchemaQualifiedTableDDL) {
    sql =>
      val parsed = ANSISQL(sql)
      if !parsed.isSuccess || !parsed.get.value.isInstanceOf[CreateGenericTable] then
        println(s"Failed to parse: $sql")
        false
      else
        val stmt = parsed.get.value.asInstanceOf[CreateGenericTable]
        val schemaIsDefined = stmt.table.schema.isDefined
        val hasQualifiedName = stmt.table.qualifiedName.contains(".")
        if !schemaIsDefined || !hasQualifiedName then
          println(s"Failed: schema=$schemaIsDefined, qualified=$hasQualifiedName for $sql")
        schemaIsDefined && hasQualifiedName
  }

  property("schema-qualified round-trip preserves schema field") = Prop.forAll(genSchemaQualifiedTableDDL) { sql =>
    import sequala.schema.SqlRenderer.toSql
    given sequala.schema.SqlFormatConfig = sequala.schema.SqlFormatConfig.Compact
    val parsed = ANSISQL(sql)
    if !parsed.isSuccess || !parsed.get.value.isInstanceOf[CreateGenericTable] then true
    else
      val stmt = parsed.get.value.asInstanceOf[CreateGenericTable]
      val generatedSql = stmt.toSql + ";"
      val reparsed = ANSISQL(generatedSql)
      if !reparsed.isSuccess || !reparsed.get.value.isInstanceOf[CreateGenericTable] then
        println(s"Failed to reparse: $generatedSql")
        false
      else
        val restmt = reparsed.get.value.asInstanceOf[CreateGenericTable]
        val schemaMatch = restmt.table.schema == stmt.table.schema
        val nameMatch = restmt.table.name == stmt.table.name
        val qualifiedNameMatch = restmt.table.qualifiedName == stmt.table.qualifiedName
        if !schemaMatch || !nameMatch || !qualifiedNameMatch then
          println(s"Original SQL: $sql")
          println(s"Generated SQL: $generatedSql")
          println(s"Original schema: ${stmt.table.schema}, name: ${stmt.table.name}")
          println(s"Reparsed schema: ${restmt.table.schema}, name: ${restmt.table.name}")
        schemaMatch && nameMatch && qualifiedNameMatch
  }

  val genOptionalSchemaTableDDL: Gen[String] = for {
    hasSchema <- Gen.oneOf(true, false)
    schema <- genIdentifier
    table <- genIdentifier
    numCols <- Gen.choose(1, 3)
    columns <- Gen.listOfN(numCols, genColumn)
    tableName = if hasSchema then s"$schema.$table" else table
  } yield s"CREATE TABLE $tableName (${columns.mkString(", ")});"

  property("schema field is None when not qualified, Some when qualified") = Prop.forAll(genOptionalSchemaTableDDL) {
    sql =>
      val parsed = ANSISQL(sql)
      if !parsed.isSuccess || !parsed.get.value.isInstanceOf[CreateGenericTable] then true
      else
        val stmt = parsed.get.value.asInstanceOf[CreateGenericTable]
        val hasQualification = sql.contains("CREATE TABLE") && {
          val afterCreate = sql.drop(sql.indexOf("CREATE TABLE") + "CREATE TABLE".length).trim
          val tableNamePart = afterCreate.takeWhile(_ != '(').trim
          tableNamePart.contains('.')
        }
        if hasQualification then stmt.table.schema.isDefined
        else stmt.table.schema.isEmpty
  }
