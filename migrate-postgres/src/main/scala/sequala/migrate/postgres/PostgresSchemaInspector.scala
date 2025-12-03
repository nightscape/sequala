package sequala.migrate.postgres

import sequala.schema.*
import sequala.schema.postgres.*
import sequala.migrate.inspect.SchemaInspector
import java.sql.{Connection, ResultSet}
import scala.collection.mutable

object PostgresSchemaInspector extends SchemaInspector[PostgresDataType, PostgresColumnOptions, PostgresTableOptions]:

  def inspectTables(
    connection: Connection,
    schemaName: String
  ): Seq[Table[PostgresDataType, PostgresColumnOptions, PostgresTableOptions]] =
    val tableNames = getTableNames(connection, schemaName)
    tableNames.flatMap(name => inspectTable(connection, schemaName, name))

  def inspectTable(
    connection: Connection,
    schemaName: String,
    tableName: String
  ): Option[Table[PostgresDataType, PostgresColumnOptions, PostgresTableOptions]] =
    val columns = getColumns(connection, schemaName, tableName)
    if columns.isEmpty then None
    else
      val primaryKey = getPrimaryKey(connection, schemaName, tableName)
      val foreignKeys = getForeignKeys(connection, schemaName, tableName)
      val uniques = getUniqueConstraints(connection, schemaName, tableName)
      val checks = getCheckConstraints(connection, schemaName, tableName)
      val indexes = getIndexes(connection, schemaName, tableName, primaryKey, uniques)
      val tableOptions = getTableOptions(connection, schemaName, tableName)
      val tableComment = getTableComment(connection, schemaName, tableName)
      val columnComments = getColumnComments(connection, schemaName, tableName)
      val columnsWithComments = columns.map { col =>
        columnComments.get(col.name) match
          case Some(comment) => col.copy(comment = Some(comment))
          case None => col
      }
      Some(
        Table(
          name = tableName,
          columns = columnsWithComments,
          primaryKey = primaryKey,
          indexes = indexes,
          foreignKeys = foreignKeys,
          checks = checks,
          uniques = uniques,
          options = tableOptions,
          comment = tableComment,
          schema = Some(schemaName)
        )
      )

  private def getTableNames(connection: Connection, schemaName: String): Seq[String] =
    val sql = """
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = ? AND table_type = 'BASE TABLE'
      ORDER BY table_name
    """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName)
    val rs = stmt.executeQuery()
    val names = mutable.Buffer[String]()
    while rs.next() do names += rs.getString("table_name")
    rs.close()
    stmt.close()
    names.toSeq

  private def getColumns(
    connection: Connection,
    schemaName: String,
    tableName: String
  ): Seq[Column[PostgresDataType, PostgresColumnOptions]] =
    val sql = """
      SELECT
        c.column_name,
        c.data_type,
        c.udt_name,
        c.character_maximum_length,
        c.numeric_precision,
        c.numeric_scale,
        c.is_nullable,
        c.column_default,
        c.is_generated,
        c.generation_expression,
        e.data_type as element_type,
        e.udt_name as element_udt_name
      FROM information_schema.columns c
      LEFT JOIN information_schema.element_types e
        ON c.table_catalog = e.object_catalog
        AND c.table_schema = e.object_schema
        AND c.table_name = e.object_name
        AND c.ordinal_position = e.collection_type_identifier::integer
      WHERE c.table_schema = ? AND c.table_name = ?
      ORDER BY c.ordinal_position
    """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName)
    stmt.setString(2, tableName)
    val rs = stmt.executeQuery()
    val columns = mutable.Buffer[Column[PostgresDataType, PostgresColumnOptions]]()
    while rs.next() do
      val name = rs.getString("column_name")
      val dataTypeName = rs.getString("data_type")
      val udtName = rs.getString("udt_name")
      val charLength = Option(rs.getObject("character_maximum_length")).map(_.toString.toInt)
      val numPrecision = Option(rs.getObject("numeric_precision")).map(_.toString.toInt)
      val numScale = Option(rs.getObject("numeric_scale")).map(_.toString.toInt)
      val elementType = Option(rs.getString("element_type"))
      val elementUdtName = Option(rs.getString("element_udt_name"))

      val dataType =
        parseDataType(dataTypeName, udtName, charLength, numPrecision, numScale, elementType, elementUdtName)
      val nullable = rs.getString("is_nullable") == "YES"
      val default = Option(rs.getString("column_default"))
      val isGenerated = rs.getString("is_generated")
      val generationExpr = Option(rs.getString("generation_expression"))

      val columnOptions = (isGenerated, generationExpr) match
        case ("ALWAYS", Some(expr)) => PostgresColumnOptions(Some(PostgresGeneratedColumn(expr, stored = true)))
        case _ => PostgresColumnOptions.empty

      columns += Column(name, dataType, nullable, default, columnOptions)
    rs.close()
    stmt.close()
    columns.toSeq

  private def parseDataType(
    typeName: String,
    udtName: String,
    charLength: Option[Int],
    numPrecision: Option[Int],
    numScale: Option[Int],
    elementType: Option[String],
    elementUdtName: Option[String]
  ): PostgresDataType =
    typeName.toLowerCase match
      case "integer" | "int" | "int4" => SqlInteger
      case "bigint" | "int8" => SqlBigInt
      case "smallint" | "int2" => SmallInt
      case "real" | "float4" => Real
      case "double precision" | "float8" => DoublePrecision
      case "boolean" | "bool" => SqlBoolean
      case "date" => SqlDate
      case "text" => SqlText
      case "character varying" | "varchar" => VarChar(charLength.getOrElse(255))
      case "character" | "char" | "bpchar" => SqlChar(charLength.getOrElse(1))
      case "numeric" | "decimal" => Decimal(numPrecision.getOrElse(10), numScale.getOrElse(0))
      case t if t.startsWith("timestamp") => SqlTimestamp(None, t.contains("with time zone"))
      case t if t.startsWith("time") => SqlTime(None, t.contains("with time zone"))
      case "uuid" => Uuid
      case "json" => Json
      case "jsonb" => Jsonb
      case "bytea" => Bytea
      case "inet" => Inet
      case "cidr" => Cidr
      case "macaddr" => MacAddr
      case "macaddr8" => MacAddr8
      case "money" => Money
      case "bit" => Bit(charLength)
      case "bit varying" => BitVarying(charLength)
      case "point" => PgPoint
      case "line" => PgLine
      case "box" => PgBox
      case "circle" => PgCircle
      case "polygon" => PgPolygon
      case "path" => PgPath
      case "tsvector" => TsVector
      case "tsquery" => TsQuery
      case "array" | "user-defined" if udtName.startsWith("_") =>
        val elementDataType = parseElementDataType(udtName.stripPrefix("_"), elementType, elementUdtName)
        PgArray(elementDataType)
      case _ =>
        udtName.toLowerCase match
          case "uuid" => Uuid
          case "json" => Json
          case "jsonb" => Jsonb
          case "bytea" => Bytea
          case "inet" => Inet
          case "cidr" => Cidr
          case "macaddr" => MacAddr
          case "macaddr8" => MacAddr8
          case "money" => Money
          case "point" => PgPoint
          case "line" => PgLine
          case "box" => PgBox
          case "circle" => PgCircle
          case "polygon" => PgPolygon
          case "path" => PgPath
          case "tsvector" => TsVector
          case "tsquery" => TsQuery
          case u if u.startsWith("_") =>
            val elementDataType = parseElementDataType(u.stripPrefix("_"), elementType, elementUdtName)
            PgArray(elementDataType)
          case "serial" | "serial4" => Serial
          case "bigserial" | "serial8" => BigSerial
          case "smallserial" | "serial2" => SmallSerial
          case _ => SqlText

  private def parseElementDataType(
    udtName: String,
    elementType: Option[String],
    elementUdtName: Option[String]
  ): PostgresDataType =
    udtName.toLowerCase match
      case "int4" | "integer" | "int" => SqlInteger
      case "int8" | "bigint" => SqlBigInt
      case "int2" | "smallint" => SmallInt
      case "float4" | "real" => Real
      case "float8" | "double precision" => DoublePrecision
      case "bool" | "boolean" => SqlBoolean
      case "date" => SqlDate
      case "text" => SqlText
      case "varchar" => VarChar(255)
      case "bpchar" | "char" => SqlChar(1)
      case "numeric" | "decimal" => Decimal(10, 0)
      case "uuid" => Uuid
      case "json" => Json
      case "jsonb" => Jsonb
      case "bytea" => Bytea
      case _ => SqlText

  private def getPrimaryKey(connection: Connection, schemaName: String, tableName: String): Option[PrimaryKey] =
    val sql = """
      SELECT tc.constraint_name, kcu.column_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
      WHERE tc.table_schema = ? AND tc.table_name = ? AND tc.constraint_type = 'PRIMARY KEY'
      ORDER BY kcu.ordinal_position
    """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName)
    stmt.setString(2, tableName)
    val rs = stmt.executeQuery()
    var constraintName: Option[String] = None
    val columns = mutable.Buffer[String]()
    while rs.next() do
      constraintName = Some(rs.getString("constraint_name"))
      columns += rs.getString("column_name")
    rs.close()
    stmt.close()
    if columns.isEmpty then None
    else Some(PrimaryKey(constraintName, columns.toSeq))

  private def getForeignKeys(connection: Connection, schemaName: String, tableName: String): Seq[ForeignKey] =
    val sql = """
      SELECT
        tc.constraint_name,
        kcu.column_name,
        ccu.table_name AS ref_table,
        ccu.column_name AS ref_column,
        rc.update_rule,
        rc.delete_rule
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
      JOIN information_schema.constraint_column_usage ccu
        ON tc.constraint_name = ccu.constraint_name AND tc.table_schema = ccu.table_schema
      JOIN information_schema.referential_constraints rc
        ON tc.constraint_name = rc.constraint_name AND tc.table_schema = rc.constraint_schema
      WHERE tc.table_schema = ? AND tc.table_name = ? AND tc.constraint_type = 'FOREIGN KEY'
      ORDER BY tc.constraint_name, kcu.ordinal_position
    """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName)
    stmt.setString(2, tableName)
    val rs = stmt.executeQuery()
    val fkMap =
      mutable.LinkedHashMap[String, (mutable.Buffer[String], String, mutable.Buffer[String], String, String)]()
    while rs.next() do
      val name = rs.getString("constraint_name")
      val col = rs.getString("column_name")
      val refTable = rs.getString("ref_table")
      val refCol = rs.getString("ref_column")
      val updateRule = rs.getString("update_rule")
      val deleteRule = rs.getString("delete_rule")
      fkMap.get(name) match
        case Some((cols, _, refCols, _, _)) =>
          if !cols.contains(col) then cols += col
          if !refCols.contains(refCol) then refCols += refCol
        case None =>
          fkMap(name) = (mutable.Buffer(col), refTable, mutable.Buffer(refCol), updateRule, deleteRule)
    rs.close()
    stmt.close()
    fkMap.map { case (name, (cols, refTable, refCols, updateRule, deleteRule)) =>
      ForeignKey(
        Some(name),
        cols.toSeq,
        refTable,
        refCols.toSeq,
        parseReferentialAction(updateRule),
        parseReferentialAction(deleteRule)
      )
    }.toSeq

  private def parseReferentialAction(action: String): ReferentialAction =
    action.toUpperCase match
      case "NO ACTION" => NoAction
      case "RESTRICT" => Restrict
      case "CASCADE" => Cascade
      case "SET NULL" => SetNull
      case "SET DEFAULT" => SetDefault
      case _ => NoAction

  private def getUniqueConstraints(connection: Connection, schemaName: String, tableName: String): Seq[Unique] =
    val sql = """
      SELECT tc.constraint_name, kcu.column_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
      WHERE tc.table_schema = ? AND tc.table_name = ? AND tc.constraint_type = 'UNIQUE'
      ORDER BY tc.constraint_name, kcu.ordinal_position
    """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName)
    stmt.setString(2, tableName)
    val rs = stmt.executeQuery()
    val uniqueMap = mutable.LinkedHashMap[String, mutable.Buffer[String]]()
    while rs.next() do
      val name = rs.getString("constraint_name")
      val col = rs.getString("column_name")
      uniqueMap.getOrElseUpdate(name, mutable.Buffer()) += col
    rs.close()
    stmt.close()
    uniqueMap.map { case (name, cols) => Unique(Some(name), cols.toSeq) }.toSeq

  private def getCheckConstraints(connection: Connection, schemaName: String, tableName: String): Seq[Check] =
    val sql = """
      SELECT tc.constraint_name, cc.check_clause
      FROM information_schema.table_constraints tc
      JOIN information_schema.check_constraints cc
        ON tc.constraint_name = cc.constraint_name AND tc.constraint_schema = cc.constraint_schema
      WHERE tc.table_schema = ? AND tc.table_name = ? AND tc.constraint_type = 'CHECK'
        AND tc.constraint_name NOT LIKE '%_not_null'
    """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName)
    stmt.setString(2, tableName)
    val rs = stmt.executeQuery()
    val checks = mutable.Buffer[Check]()
    while rs.next() do
      val name = rs.getString("constraint_name")
      val expr = rs.getString("check_clause")
      checks += Check(Some(name), expr)
    rs.close()
    stmt.close()
    checks.toSeq

  private def getIndexes(
    connection: Connection,
    schemaName: String,
    tableName: String,
    primaryKey: Option[PrimaryKey],
    uniques: Seq[Unique]
  ): Seq[Index] =
    val pkConstraintNames = primaryKey.flatMap(_.name).toSet
    val uniqueConstraintNames = uniques.flatMap(_.name).toSet
    val excludedNames = pkConstraintNames ++ uniqueConstraintNames

    val sql = """
      SELECT
        i.relname AS index_name,
        ix.indisunique AS is_unique,
        a.attname AS column_name,
        array_position(ix.indkey, a.attnum) AS column_position,
        (ix.indoption[array_position(ix.indkey, a.attnum)::int])::int & 1 = 1 AS is_desc,
        (ix.indoption[array_position(ix.indkey, a.attnum)::int])::int & 2 = 2 AS nulls_first,
        pg_get_expr(ix.indpred, ix.indrelid) AS where_clause
      FROM pg_index ix
      JOIN pg_class t ON t.oid = ix.indrelid
      JOIN pg_class i ON i.oid = ix.indexrelid
      JOIN pg_namespace n ON n.oid = t.relnamespace
      JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
      WHERE n.nspname = ? AND t.relname = ?
        AND NOT ix.indisprimary
      ORDER BY i.relname, array_position(ix.indkey, a.attnum)
    """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName)
    stmt.setString(2, tableName)
    val rs = stmt.executeQuery()
    val indexMap = mutable.LinkedHashMap[String, (Boolean, mutable.Buffer[IndexColumn], Option[String])]()
    while rs.next() do
      val indexName = rs.getString("index_name")
      if !excludedNames.contains(indexName) then
        val isUnique = rs.getBoolean("is_unique")
        val columnName = rs.getString("column_name")
        val isDesc = rs.getBoolean("is_desc")
        val nullsFirst = rs.getBoolean("nulls_first")
        val whereClause = Option(rs.getString("where_clause"))

        val indexCol = IndexColumn(columnName, isDesc, Some(nullsFirst))
        indexMap.get(indexName) match
          case Some((_, cols, _)) => cols += indexCol
          case None => indexMap(indexName) = (isUnique, mutable.Buffer(indexCol), whereClause)
    rs.close()
    stmt.close()
    indexMap.map { case (name, (isUnique, cols, where)) =>
      Index(Some(name), cols.toSeq, isUnique, where)
    }.toSeq

  private def getTableOptions(connection: Connection, schemaName: String, tableName: String): PostgresTableOptions =
    val partitionSpec = getPartitionSpec(connection, schemaName, tableName)
    val inherits = getInherits(connection, schemaName, tableName)
    val withOptions = getWithOptions(connection, schemaName, tableName)
    PostgresTableOptions(inherits = inherits, partitionBy = partitionSpec, using = None, withOptions = withOptions)

  private def getPartitionSpec(
    connection: Connection,
    schemaName: String,
    tableName: String
  ): Option[PostgresPartitionSpec] =
    val sql = """
      SELECT
        pt.partstrat,
        array_agg(a.attname ORDER BY array_position(pt.partattrs::int[], a.attnum)) AS partition_columns
      FROM pg_partitioned_table pt
      JOIN pg_class c ON c.oid = pt.partrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(pt.partattrs::int[])
      WHERE n.nspname = ? AND c.relname = ?
      GROUP BY pt.partstrat
    """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName)
    stmt.setString(2, tableName)
    val rs = stmt.executeQuery()
    val result =
      if rs.next() then
        val strategyChar = rs.getString("partstrat")
        val columnsArray = rs.getArray("partition_columns")
        val columns = columnsArray.getArray.asInstanceOf[Array[String]].toSeq
        val strategy = strategyChar match
          case "r" => PostgresPartitionByRange
          case "l" => PostgresPartitionByList
          case "h" => PostgresPartitionByHash
          case _ => PostgresPartitionByRange
        Some(PostgresPartitionSpec(strategy, columns))
      else None
    rs.close()
    stmt.close()
    result

  private def getInherits(connection: Connection, schemaName: String, tableName: String): Seq[String] =
    val sql = """
      SELECT pc.relname AS parent_table
      FROM pg_inherits i
      JOIN pg_class c ON c.oid = i.inhrelid
      JOIN pg_class pc ON pc.oid = i.inhparent
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = ? AND c.relname = ?
      ORDER BY i.inhseqno
    """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName)
    stmt.setString(2, tableName)
    val rs = stmt.executeQuery()
    val parents = mutable.Buffer[String]()
    while rs.next() do parents += rs.getString("parent_table")
    rs.close()
    stmt.close()
    parents.toSeq

  private def getWithOptions(connection: Connection, schemaName: String, tableName: String): Map[String, String] =
    val sql = """
      SELECT unnest(c.reloptions) AS option
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = ? AND c.relname = ?
    """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName)
    stmt.setString(2, tableName)
    val rs = stmt.executeQuery()
    val options = mutable.Map[String, String]()
    while rs.next() do
      val opt = rs.getString("option")
      val parts = opt.split("=", 2)
      if parts.length == 2 then options(parts(0)) = parts(1)
    rs.close()
    stmt.close()
    options.toMap

  private def getTableComment(connection: Connection, schemaName: String, tableName: String): Option[String] =
    val sql = """
      SELECT obj_description(c.oid) AS comment
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = ? AND c.relname = ?
    """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName)
    stmt.setString(2, tableName)
    val rs = stmt.executeQuery()
    val comment = if rs.next() then Option(rs.getString("comment")) else None
    rs.close()
    stmt.close()
    comment

  private def getColumnComments(connection: Connection, schemaName: String, tableName: String): Map[String, String] =
    val sql = """
      SELECT a.attname AS column_name, col_description(c.oid, a.attnum) AS comment
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_attribute a ON a.attrelid = c.oid
      WHERE n.nspname = ? AND c.relname = ? AND a.attnum > 0 AND NOT a.attisdropped
        AND col_description(c.oid, a.attnum) IS NOT NULL
    """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName)
    stmt.setString(2, tableName)
    val rs = stmt.executeQuery()
    val comments = mutable.Map[String, String]()
    while rs.next() do
      val colName = rs.getString("column_name")
      val comment = rs.getString("comment")
      if comment != null then comments(colName) = comment
    rs.close()
    stmt.close()
    comments.toMap
