package sequala.migrate.inspect

import sequala.schema.*
import java.sql.{Connection, ResultSet}
import scala.collection.mutable

object OracleInspector extends SchemaInspector[CommonDataType, NoColumnOptions.type, NoTableOptions.type]:

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
      FROM all_tables
      WHERE owner = ?
      ORDER BY table_name
    """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName.toUpperCase)
    val rs = stmt.executeQuery()
    val names = mutable.Buffer[String]()
    while rs.next() do names += rs.getString("TABLE_NAME")
    rs.close()
    stmt.close()
    names.toSeq

  private def getColumns(
    connection: Connection,
    schemaName: String,
    tableName: String
  ): Seq[Column[CommonDataType, NoColumnOptions.type]] =
    val sql = """
      SELECT
        column_name,
        data_type,
        data_length,
        data_precision,
        data_scale,
        nullable,
        data_default
      FROM all_tab_columns
      WHERE owner = ? AND table_name = ?
      ORDER BY column_id
    """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName.toUpperCase)
    stmt.setString(2, tableName) // Don't uppercase - preserve exact case from all_tables
    val rs = stmt.executeQuery()
    val columns = mutable.Buffer[Column[CommonDataType, NoColumnOptions.type]]()
    while rs.next() do
      // DATA_DEFAULT is a LONG column and must be read first to avoid ORA-17027
      val default = Option(rs.getString("DATA_DEFAULT")).map(_.trim)
      val name = rs.getString("COLUMN_NAME")
      val dataType = parseDataType(
        rs.getString("DATA_TYPE"),
        Option(rs.getObject("DATA_LENGTH")).map(_.toString.toInt),
        Option(rs.getObject("DATA_PRECISION")).map(_.toString.toInt),
        Option(rs.getObject("DATA_SCALE")).map(_.toString.toInt)
      )
      val nullable = rs.getString("NULLABLE") == "Y"
      columns += Column(name, dataType, nullable, default, NoColumnOptions)
    rs.close()
    stmt.close()
    columns.toSeq

  private def parseDataType(
    typeName: String,
    dataLength: Option[Int],
    dataPrecision: Option[Int],
    dataScale: Option[Int]
  ): CommonDataType =
    typeName.toUpperCase match
      case "NUMBER" =>
        (dataPrecision, dataScale) match
          case (Some(1), Some(0)) => SqlBoolean
          case (Some(p), Some(0)) if p <= 10 => SqlInteger
          case (Some(p), Some(0)) if p > 10 => SqlBigInt
          case (Some(p), Some(s)) => Decimal(p, s)
          case (Some(p), None) => Decimal(p, 0)
          case (None, _) => Decimal(38, 0)
      case "INTEGER" | "INT" => SqlInteger
      case "SMALLINT" => SmallInt
      case "FLOAT" | "BINARY_FLOAT" => Real
      case "BINARY_DOUBLE" => DoublePrecision
      case "VARCHAR2" | "NVARCHAR2" => VarChar(dataLength.getOrElse(4000))
      case "CHAR" | "NCHAR" => SqlChar(dataLength.getOrElse(1))
      case "CLOB" | "NCLOB" | "LONG" => SqlText
      case "DATE" => SqlDate
      case t if t.startsWith("TIMESTAMP") => SqlTimestamp(None, t.contains("WITH TIME ZONE"))
      case "BOOLEAN" => SqlBoolean
      case _ => SqlText

  private def getPrimaryKey(connection: Connection, schemaName: String, tableName: String): Option[PrimaryKey] =
    val sql = """
      SELECT cons.constraint_name, cols.column_name
      FROM all_constraints cons
      JOIN all_cons_columns cols
        ON cons.constraint_name = cols.constraint_name
        AND cons.owner = cols.owner
      WHERE cons.owner = ? AND cons.table_name = ? AND cons.constraint_type = 'P'
      ORDER BY cols.position
    """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName.toUpperCase)
    stmt.setString(2, tableName) // Don't uppercase - preserve exact case from all_tables
    val rs = stmt.executeQuery()
    var constraintName: Option[String] = None
    val columns = mutable.Buffer[String]()
    while rs.next() do
      constraintName = Some(rs.getString("CONSTRAINT_NAME"))
      columns += rs.getString("COLUMN_NAME")
    rs.close()
    stmt.close()
    if columns.isEmpty then None
    else Some(PrimaryKey(constraintName, columns.toSeq))

  private def getForeignKeys(connection: Connection, schemaName: String, tableName: String): Seq[ForeignKey] =
    val sql = """
      SELECT
        cons.constraint_name,
        cols.column_name,
        r_cons.table_name AS ref_table,
        r_cols.column_name AS ref_column,
        cons.delete_rule
      FROM all_constraints cons
      JOIN all_cons_columns cols
        ON cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner
      JOIN all_constraints r_cons
        ON cons.r_constraint_name = r_cons.constraint_name AND cons.r_owner = r_cons.owner
      JOIN all_cons_columns r_cols
        ON r_cons.constraint_name = r_cols.constraint_name
        AND r_cons.owner = r_cols.owner
        AND cols.position = r_cols.position
      WHERE cons.owner = ? AND cons.table_name = ? AND cons.constraint_type = 'R'
      ORDER BY cons.constraint_name, cols.position
    """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName.toUpperCase)
    stmt.setString(2, tableName) // Don't uppercase - preserve exact case from all_tables
    val rs = stmt.executeQuery()
    val fkMap =
      mutable.LinkedHashMap[String, (mutable.Buffer[String], String, mutable.Buffer[String], String)]()
    while rs.next() do
      val name = rs.getString("CONSTRAINT_NAME")
      val col = rs.getString("COLUMN_NAME")
      val refTable = rs.getString("REF_TABLE")
      val refCol = rs.getString("REF_COLUMN")
      val deleteRule = Option(rs.getString("DELETE_RULE")).getOrElse("NO ACTION")
      fkMap.get(name) match
        case Some((cols, _, refCols, _)) =>
          if !cols.contains(col) then cols += col
          if !refCols.contains(refCol) then refCols += refCol
        case None =>
          fkMap(name) = (mutable.Buffer(col), refTable, mutable.Buffer(refCol), deleteRule)
    rs.close()
    stmt.close()
    fkMap.map { case (name, (cols, refTable, refCols, deleteRule)) =>
      ForeignKey(Some(name), cols.toSeq, refTable, refCols.toSeq, NoAction, parseReferentialAction(deleteRule))
    }.toSeq

  private def parseReferentialAction(action: String): ReferentialAction =
    action.toUpperCase match
      case "NO ACTION" => NoAction
      case "CASCADE" => Cascade
      case "SET NULL" => SetNull
      case "SET DEFAULT" => SetDefault
      case _ => NoAction

  private def getUniqueConstraints(connection: Connection, schemaName: String, tableName: String): Seq[Unique] =
    val sql = """
      SELECT cons.constraint_name, cols.column_name
      FROM all_constraints cons
      JOIN all_cons_columns cols
        ON cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner
      WHERE cons.owner = ? AND cons.table_name = ? AND cons.constraint_type = 'U'
      ORDER BY cons.constraint_name, cols.position
    """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName.toUpperCase)
    stmt.setString(2, tableName) // Don't uppercase - preserve exact case from all_tables
    val rs = stmt.executeQuery()
    val uniqueMap = mutable.LinkedHashMap[String, mutable.Buffer[String]]()
    while rs.next() do
      val name = rs.getString("CONSTRAINT_NAME")
      val col = rs.getString("COLUMN_NAME")
      uniqueMap.getOrElseUpdate(name, mutable.Buffer()) += col
    rs.close()
    stmt.close()
    uniqueMap.map { case (name, cols) => Unique(Some(name), cols.toSeq) }.toSeq

  private def getCheckConstraints(connection: Connection, schemaName: String, tableName: String): Seq[Check] =
    val sql = """
      SELECT constraint_name, search_condition
      FROM all_constraints
      WHERE owner = ? AND table_name = ? AND constraint_type = 'C'
        AND generated = 'USER NAME'
    """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName.toUpperCase)
    stmt.setString(2, tableName) // Don't uppercase - preserve exact case from all_tables
    val rs = stmt.executeQuery()
    val checks = mutable.Buffer[Check]()
    while rs.next() do
      // SEARCH_CONDITION is a LONG column and must be read first to avoid ORA-17027
      val expr = rs.getString("SEARCH_CONDITION")
      val name = rs.getString("CONSTRAINT_NAME")
      if expr != null && !expr.contains("IS NOT NULL") then checks += Check(Some(name), expr)
    rs.close()
    stmt.close()
    checks.toSeq

  private def getIndexes(connection: Connection, schemaName: String, tableName: String): Seq[Index] =
    val sql = """
      SELECT
        i.index_name,
        ic.column_name,
        i.uniqueness,
        ic.descend
      FROM all_indexes i
      JOIN all_ind_columns ic
        ON i.index_name = ic.index_name AND i.owner = ic.index_owner
      WHERE i.owner = ? AND i.table_name = ?
        AND NOT EXISTS (
          SELECT 1 FROM all_constraints c
          WHERE c.index_name = i.index_name
            AND c.owner = i.owner
            AND c.constraint_type IN ('P', 'U')
        )
      ORDER BY i.index_name, ic.column_position
    """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName.toUpperCase)
    stmt.setString(2, tableName) // Don't uppercase - preserve exact case from all_tables
    val rs = stmt.executeQuery()
    val indexMap = mutable.LinkedHashMap[String, (mutable.Buffer[IndexColumn], Boolean)]()
    while rs.next() do
      val indexName = rs.getString("INDEX_NAME")
      val columnName = rs.getString("COLUMN_NAME")
      val isUnique = rs.getString("UNIQUENESS") == "UNIQUE"
      val descending = rs.getString("DESCEND") == "DESC"
      val indexCol = IndexColumn(columnName, descending, None)
      indexMap.get(indexName) match
        case Some((cols, _)) => cols += indexCol
        case None => indexMap(indexName) = (mutable.Buffer(indexCol), isUnique)
    rs.close()
    stmt.close()
    indexMap.map { case (name, (cols, isUnique)) =>
      Index(Some(name), cols.toSeq, isUnique, None)
    }.toSeq
