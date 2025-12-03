package sequala.migrate.oracle

import sequala.schema.*
import sequala.schema.oracle.*
import sequala.migrate.inspect.SchemaInspector
import java.sql.{Connection, ResultSet}
import scala.collection.mutable

object OracleSchemaInspector extends SchemaInspector[OracleDataType, OracleColumnOptions, OracleTableOptions]:

  def inspectTables(
    connection: Connection,
    schemaName: String
  ): Seq[Table[OracleDataType, OracleColumnOptions, OracleTableOptions]] =
    val tableNames = getTableNames(connection, schemaName)
    tableNames.flatMap(name => inspectTable(connection, schemaName, name))

  def inspectTable(
    connection: Connection,
    schemaName: String,
    tableName: String
  ): Option[Table[OracleDataType, OracleColumnOptions, OracleTableOptions]] =
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

  def getSchemaNames(connection: Connection, schemaPattern: Option[String]): Seq[String] =
    val sql = schemaPattern match
      case Some(_) => """
        SELECT DISTINCT owner
        FROM all_tables
        WHERE owner LIKE ?
        ORDER BY owner
      """
      case None => """
        SELECT DISTINCT owner
        FROM all_tables
        ORDER BY owner
      """
    val stmt = connection.prepareStatement(sql)
    schemaPattern.foreach(p => stmt.setString(1, p.toUpperCase))
    val rs = stmt.executeQuery()
    val names = mutable.Buffer[String]()
    while rs.next() do names += rs.getString("OWNER")
    rs.close()
    stmt.close()
    names.toSeq

  private def getTableNames(connection: Connection, schemaName: String): Seq[String] =
    getTableNames(connection, schemaName, None)

  def getTableNames(connection: Connection, schemaName: String, tablePattern: Option[String]): Seq[String] =
    val sql = tablePattern match
      case Some(_) => """
        SELECT table_name
        FROM all_tables
        WHERE owner = ? AND table_name LIKE ?
        ORDER BY table_name
      """
      case None => """
        SELECT table_name
        FROM all_tables
        WHERE owner = ?
        ORDER BY table_name
      """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName.toUpperCase)
    tablePattern.foreach(p => stmt.setString(2, p.toUpperCase))
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
  ): Seq[Column[OracleDataType, OracleColumnOptions]] =
    val sql = """
      SELECT
        column_name,
        data_type,
        data_length,
        char_length,
        data_precision,
        data_scale,
        nullable,
        data_default,
        char_used,
        virtual_column,
        hidden_column
      FROM all_tab_cols
      WHERE owner = ? AND table_name = ?
        AND hidden_column = 'NO'
      ORDER BY column_id
    """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName.toUpperCase)
    stmt.setString(2, tableName.toUpperCase)
    val rs = stmt.executeQuery()
    val columns = mutable.Buffer[Column[OracleDataType, OracleColumnOptions]]()
    while rs.next() do
      // DATA_DEFAULT is a LONG column and must be read first to avoid ORA-17027
      val default = Option(rs.getString("DATA_DEFAULT")).map(_.trim)
      val name = rs.getString("COLUMN_NAME")
      val dataTypeName = rs.getString("DATA_TYPE")
      val dataLength = Option(rs.getObject("DATA_LENGTH")).map(_.toString.toInt)
      val charLength = Option(rs.getObject("CHAR_LENGTH")).map(_.toString.toInt)
      val dataPrecision = Option(rs.getObject("DATA_PRECISION")).map(_.toString.toInt)
      val dataScale = Option(rs.getObject("DATA_SCALE")).map(_.toString.toInt)
      val charUsed = Option(rs.getString("CHAR_USED")).getOrElse("B")
      val virtualColumn = rs.getString("VIRTUAL_COLUMN")

      // Use char_length for CHAR semantics, data_length for BYTE semantics
      val effectiveLength = if charUsed == "C" then charLength.orElse(dataLength) else dataLength
      val dataType = parseDataType(dataTypeName, effectiveLength, dataPrecision, dataScale, charUsed)
      val nullable = rs.getString("NULLABLE") == "Y"

      val columnOptions =
        if virtualColumn == "YES" then OracleColumnOptions(virtual = default)
        else OracleColumnOptions.empty

      val actualDefault = if virtualColumn == "YES" then None else default

      columns += Column(name, dataType, nullable, actualDefault, columnOptions)
    rs.close()
    stmt.close()
    columns.toSeq

  private def parseDataType(
    typeName: String,
    dataLength: Option[Int],
    dataPrecision: Option[Int],
    dataScale: Option[Int],
    charUsed: String
  ): OracleDataType =
    val sizeSemantics = if charUsed == "C" then Chars else Bytes
    typeName.toUpperCase match
      case "VARCHAR2" => Varchar2(dataLength.getOrElse(4000), sizeSemantics)
      case "NVARCHAR2" => NVarchar2(dataLength.getOrElse(4000))
      case "CHAR" => OracleChar(dataLength.getOrElse(1), sizeSemantics)
      case "NCHAR" => NChar(dataLength.getOrElse(1))
      case "NUMBER" => Number(dataPrecision, dataScale)
      case "FLOAT" => Number(dataPrecision, None)
      case "BINARY_FLOAT" => BinaryFloat
      case "BINARY_DOUBLE" => BinaryDouble
      case "INTEGER" | "INT" => SqlInteger
      case "SMALLINT" => SmallInt
      case "RAW" => Raw(dataLength.getOrElse(2000))
      case "LONG" => OracleLong
      case "LONG RAW" => LongRaw
      case "ROWID" => OracleRowid
      case "UROWID" => URowid(dataLength)
      case "DATE" => SqlDate
      case t if t.startsWith("TIMESTAMP") =>
        val withTz = t.contains("WITH TIME ZONE")
        // Normalize: precision 6 is Oracle's fixed default, treat as None for canonical comparison
        val normalizedPrecision = dataPrecision.filterNot(_ == 6)
        SqlTimestamp(normalizedPrecision, withTz)
      case t if t.startsWith("INTERVAL YEAR") =>
        IntervalYearToMonth(dataPrecision)
      case t if t.startsWith("INTERVAL DAY") =>
        IntervalDayToSecond(dataPrecision, dataScale)
      case "CLOB" => SqlClob
      case "NCLOB" => SqlClob
      case "BLOB" => SqlBlob
      case "BFILE" => SqlBlob
      case "XMLTYPE" => XMLType
      case "BOOLEAN" => SqlBoolean
      case _ => Varchar2(dataLength.getOrElse(4000), sizeSemantics)

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
    stmt.setString(2, tableName.toUpperCase)
    val rs = stmt.executeQuery()
    var constraintName: Option[String] = None
    val columns = mutable.Buffer[String]()
    while rs.next() do
      constraintName = Some(rs.getString("CONSTRAINT_NAME"))
      columns += rs.getString("COLUMN_NAME")
    rs.close()
    stmt.close()
    if columns.isEmpty then None
    else
      // Normalize: strip system-generated constraint names (SYS_C*) for canonical comparison
      val normalizedName = constraintName.filterNot(_.startsWith("SYS_C"))
      Some(PrimaryKey(normalizedName, columns.toSeq))

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
    stmt.setString(2, tableName.toUpperCase)
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
    stmt.setString(2, tableName.toUpperCase)
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
    stmt.setString(2, tableName.toUpperCase)
    val rs = stmt.executeQuery()
    val checks = mutable.Buffer[Check]()
    while rs.next() do
      // LONG column must be read first to avoid ORA-17027 "Stream has already been closed"
      val expr = rs.getString("SEARCH_CONDITION")
      val name = rs.getString("CONSTRAINT_NAME")
      if expr != null && !expr.contains("IS NOT NULL") then checks += Check(Some(name), expr)
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

    val constraintIndexNames = getConstraintIndexNames(connection, schemaName, tableName)
    val excludedNames = pkConstraintNames ++ uniqueConstraintNames ++ constraintIndexNames

    val sql = """
      SELECT
        idx.index_name,
        idx.uniqueness,
        col.column_name,
        col.column_position,
        col.descend
      FROM all_indexes idx
      JOIN all_ind_columns col
        ON idx.index_name = col.index_name AND idx.owner = col.index_owner
      WHERE idx.table_owner = ? AND idx.table_name = ?
        AND idx.index_type IN ('NORMAL', 'BITMAP', 'FUNCTION-BASED NORMAL')
      ORDER BY idx.index_name, col.column_position
    """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName.toUpperCase)
    stmt.setString(2, tableName.toUpperCase)
    val rs = stmt.executeQuery()
    val indexMap = mutable.LinkedHashMap[String, (Boolean, mutable.Buffer[IndexColumn])]()
    while rs.next() do
      val indexName = rs.getString("INDEX_NAME")
      if !excludedNames.contains(indexName) then
        val isUnique = rs.getString("UNIQUENESS") == "UNIQUE"
        val columnName = rs.getString("COLUMN_NAME")
        val isDesc = rs.getString("DESCEND") == "DESC"

        val indexCol = IndexColumn(columnName, isDesc, None)
        indexMap.get(indexName) match
          case Some((_, cols)) => cols += indexCol
          case None => indexMap(indexName) = (isUnique, mutable.Buffer(indexCol))
    rs.close()
    stmt.close()
    indexMap.map { case (name, (isUnique, cols)) =>
      Index(Some(name), cols.toSeq, isUnique, None)
    }.toSeq

  private def getConstraintIndexNames(connection: Connection, schemaName: String, tableName: String): Set[String] =
    val sql = """
      SELECT index_name
      FROM all_constraints
      WHERE owner = ? AND table_name = ?
        AND constraint_type IN ('P', 'U')
        AND index_name IS NOT NULL
    """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName.toUpperCase)
    stmt.setString(2, tableName.toUpperCase)
    val rs = stmt.executeQuery()
    val names = mutable.Set[String]()
    while rs.next() do names += rs.getString("INDEX_NAME")
    rs.close()
    stmt.close()
    names.toSet

  private def getTableComment(connection: Connection, schemaName: String, tableName: String): Option[String] =
    val sql = """
      SELECT comments
      FROM all_tab_comments
      WHERE owner = ? AND table_name = ? AND comments IS NOT NULL
    """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName.toUpperCase)
    stmt.setString(2, tableName.toUpperCase)
    val rs = stmt.executeQuery()
    val comment = if rs.next() then Option(rs.getString("COMMENTS")) else None
    rs.close()
    stmt.close()
    comment

  private def getColumnComments(connection: Connection, schemaName: String, tableName: String): Map[String, String] =
    val sql = """
      SELECT column_name, comments
      FROM all_col_comments
      WHERE owner = ? AND table_name = ? AND comments IS NOT NULL
    """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName.toUpperCase)
    stmt.setString(2, tableName.toUpperCase)
    val rs = stmt.executeQuery()
    val comments = mutable.Map[String, String]()
    while rs.next() do
      val colName = rs.getString("COLUMN_NAME")
      val comment = rs.getString("COMMENTS")
      if comment != null then comments(colName) = comment
    rs.close()
    stmt.close()
    comments.toMap

  private def getTableOptions(connection: Connection, schemaName: String, tableName: String): OracleTableOptions =
    val sql = """
      SELECT
        tablespace_name,
        pct_free,
        pct_used,
        ini_trans,
        max_trans,
        logging,
        compression,
        compress_for,
        cache,
        row_movement
      FROM all_tables
      WHERE owner = ? AND table_name = ?
    """
    val stmt = connection.prepareStatement(sql)
    stmt.setString(1, schemaName.toUpperCase)
    stmt.setString(2, tableName.toUpperCase)
    val rs = stmt.executeQuery()
    val options =
      if rs.next() then
        val tablespace = Option(rs.getString("TABLESPACE_NAME"))
        val pctFree = Option(rs.getObject("PCT_FREE")).map(_.toString.toInt)
        val pctUsed = Option(rs.getObject("PCT_USED")).map(_.toString.toInt)
        val iniTrans = Option(rs.getObject("INI_TRANS")).map(_.toString.toInt)
        val maxTrans = Option(rs.getObject("MAX_TRANS")).map(_.toString.toInt)
        val logging = Option(rs.getString("LOGGING")).map(_ == "YES")
        val compression = Option(rs.getString("COMPRESSION")).filter(_ == "ENABLED")
        val compressFor = Option(rs.getString("COMPRESS_FOR"))
        val cache = Option(rs.getString("CACHE")).map(_.trim == "Y")
        val rowMovement = Option(rs.getString("ROW_MOVEMENT")).map(_ == "ENABLED")

        val compress = compression.flatMap { _ =>
          compressFor.map {
            case "BASIC" => OracleCompressBasic
            case "ADVANCED" => OracleCompressAdvanced
            case other => OracleCompressFor(other)
          }
        }

        OracleTableOptions(
          tablespace = tablespace.filterNot(_ == "USERS"),
          pctFree = pctFree.filterNot(_ == 10),
          pctUsed = pctUsed,
          iniTrans = iniTrans.filterNot(_ == 1),
          maxTrans = maxTrans.filterNot(_ == 255),
          storage = None,
          logging = logging.filterNot(_ == true),
          compress = compress,
          cache = cache.filterNot(_ == false),
          parallel = None,
          rowMovement = rowMovement.filterNot(_ == false)
        )
      else OracleTableOptions.empty
    rs.close()
    stmt.close()
    options
