package sequala.migrate.inspect

import sequala.schema.*
import java.sql.{Connection, ResultSet}
import scala.collection.mutable

object PostgresInspector extends SchemaInspector[CommonDataType, NoColumnOptions.type, NoTableOptions.type]:

  def inspectTables(
    connection: Connection,
    schemaName: String
  ): Seq[Table[CommonDataType, NoColumnOptions.type, NoTableOptions.type]] =
    val tableNames = getTableNames(connection, schemaName)
    tableNames.flatMap(name => inspectTable(connection, schemaName, name))

  def inspectTable(
    connection: Connection,
    schemaName: String,
    tableName: String
  ): Option[Table[CommonDataType, NoColumnOptions.type, NoTableOptions.type]] =
    val columns = getColumns(connection, schemaName, tableName)
    if columns.isEmpty then None
    else
      val primaryKey = getPrimaryKey(connection, schemaName, tableName)
      val foreignKeys = getForeignKeys(connection, schemaName, tableName)
      val uniques = getUniqueConstraints(connection, schemaName, tableName)
      val checks = getCheckConstraints(connection, schemaName, tableName)
      val indexes = getIndexes(connection, schemaName, tableName)
      Some(Table(tableName, columns, primaryKey, indexes, foreignKeys, checks, uniques, NoTableOptions))

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
  ): Seq[Column[CommonDataType, NoColumnOptions.type]] =
    val sql = """
      SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale,
             is_nullable, column_default
      FROM information_schema.columns
      WHERE table_schema = ? AND table_name = ?
      ORDER BY ordinal_position
    """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName)
    stmt.setString(2, tableName)
    val rs = stmt.executeQuery()
    val columns = mutable.Buffer[Column[CommonDataType, NoColumnOptions.type]]()
    while rs.next() do
      val name = rs.getString("column_name")
      val dataType = parseDataType(
        rs.getString("data_type"),
        Option(rs.getObject("character_maximum_length")).map(_.toString.toInt),
        Option(rs.getObject("numeric_precision")).map(_.toString.toInt),
        Option(rs.getObject("numeric_scale")).map(_.toString.toInt)
      )
      val nullable = rs.getString("is_nullable") == "YES"
      val default = Option(rs.getString("column_default"))
      columns += Column(name, dataType, nullable, default, NoColumnOptions)
    rs.close()
    stmt.close()
    columns.toSeq

  private def parseDataType(
    typeName: String,
    charLength: Option[Int],
    numPrecision: Option[Int],
    numScale: Option[Int]
  ): CommonDataType =
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
      case "character" | "char" => SqlChar(charLength.getOrElse(1))
      case "numeric" | "decimal" => Decimal(numPrecision.getOrElse(10), numScale.getOrElse(0))
      case t if t.startsWith("timestamp") => SqlTimestamp(None, t.contains("with time zone"))
      case t if t.startsWith("time") => SqlTime(None, t.contains("with time zone"))
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

  private def getIndexes(connection: Connection, schemaName: String, tableName: String): Seq[Index] =
    val sql = """
      SELECT
        i.relname AS index_name,
        a.attname AS column_name,
        am.amname AS index_type,
        ix.indisunique AS is_unique,
        ix.indoption[array_position(ix.indkey, a.attnum) - 1] AS options,
        pg_get_expr(ix.indpred, ix.indrelid) AS predicate
      FROM pg_index ix
      JOIN pg_class i ON ix.indexrelid = i.oid
      JOIN pg_class t ON ix.indrelid = t.oid
      JOIN pg_namespace n ON t.relnamespace = n.oid
      JOIN pg_am am ON i.relam = am.oid
      JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
      WHERE n.nspname = ? AND t.relname = ?
        AND NOT ix.indisprimary
        AND NOT EXISTS (
          SELECT 1 FROM pg_constraint c
          WHERE c.conindid = i.oid AND c.contype = 'u'
        )
      ORDER BY i.relname, array_position(ix.indkey, a.attnum)
    """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName)
    stmt.setString(2, tableName)
    val rs = stmt.executeQuery()
    val indexMap = mutable.LinkedHashMap[String, (mutable.Buffer[IndexColumn], Boolean, Option[String])]()
    while rs.next() do
      val indexName = rs.getString("index_name")
      val columnName = rs.getString("column_name")
      val isUnique = rs.getBoolean("is_unique")
      val options = rs.getInt("options")
      val predicate = Option(rs.getString("predicate"))
      val descending = (options & 1) != 0
      val indexCol = IndexColumn(columnName, descending, None)
      indexMap.get(indexName) match
        case Some((cols, _, _)) => cols += indexCol
        case None => indexMap(indexName) = (mutable.Buffer(indexCol), isUnique, predicate)
    rs.close()
    stmt.close()
    indexMap.map { case (name, (cols, isUnique, predicate)) =>
      Index(Some(name), cols.toSeq, isUnique, predicate)
    }.toSeq
