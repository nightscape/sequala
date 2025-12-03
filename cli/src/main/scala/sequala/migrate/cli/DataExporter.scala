package sequala.migrate.cli

import java.sql.{Connection, ResultSet, Types}
import scala.collection.mutable
import io.circe.{Json, JsonObject}
import io.circe.syntax.*
import sequala.schema.Insert
import sequala.schema.ast.{
  DoublePrimitive,
  ExplicitInsert,
  Expression,
  LongPrimitive,
  Name,
  NullPrimitive,
  StringPrimitive
}

object DataExporter:

  def exportTableData(
    connection: Connection,
    schemaName: String,
    tableName: String,
    batchSize: Int,
    dialect: String
  ): Iterator[String] =
    val qualifiedName = dialect match
      case "oracle" => s""""${schemaName.toUpperCase}"."${tableName.toUpperCase}""""
      case _ => s""""$schemaName"."$tableName""""

    val stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    if dialect != "oracle" then stmt.setFetchSize(batchSize)
    val rs = stmt.executeQuery(s"SELECT * FROM $qualifiedName")
    val meta = rs.getMetaData
    val columnCount = meta.getColumnCount
    val columnNames = (1 to columnCount)
      .map { i =>
        val name = meta.getColumnName(i)
        dialect match
          case "oracle" => s""""$name""""
          case _ => s""""$name""""
      }
      .mkString(", ")
    val columnTypes = (1 to columnCount).map(meta.getColumnType).toArray

    new Iterator[String]:
      private var hasMore = rs.next()

      def hasNext: Boolean = hasMore

      def next(): String =
        val values = new StringBuilder
        var rowCount = 0

        while hasMore && rowCount < batchSize do
          if rowCount > 0 then values.append(",\n  ")
          values.append("(")
          for i <- 1 to columnCount do
            if i > 1 then values.append(", ")
            values.append(formatValue(rs.getObject(i), columnTypes(i - 1), dialect))
          values.append(")")
          rowCount += 1
          hasMore = rs.next()

        if !hasMore then
          rs.close()
          stmt.close()

        s"INSERT INTO $qualifiedName ($columnNames) VALUES\n  ${values.toString};"

  private def formatValue(value: Any, sqlType: Int, dialect: String): String = value match
    case null => "NULL"
    case s: String => escapeString(s)
    case n: java.lang.Number => n.toString
    case b: java.lang.Boolean => if b then "1" else "0"
    case d: java.sql.Date =>
      dialect match
        case "oracle" => s"DATE '${d.toString}'"
        case _ => s"'${d.toString}'"
    case t: java.sql.Timestamp =>
      dialect match
        case "oracle" => s"TO_TIMESTAMP('${t.toString}', 'YYYY-MM-DD HH24:MI:SS.FF')"
        case _ => s"'${t.toString}'::timestamp"
    case bytes: Array[Byte] =>
      dialect match
        case "oracle" => "HEXTORAW('" + bytes.map(b => f"${b & 0xff}%02X").mkString + "')"
        case _ => "'\\x" + bytes.map(b => f"${b & 0xff}%02X").mkString + "'::bytea"
    case clob: java.sql.Clob =>
      val content = clob.getSubString(1, clob.length().toInt)
      escapeString(content)
    case blob: java.sql.Blob =>
      val bytes = blob.getBytes(1, blob.length().toInt)
      dialect match
        case "oracle" => "HEXTORAW('" + bytes.map(b => f"${b & 0xff}%02X").mkString + "')"
        case _ => "'\\x" + bytes.map(b => f"${b & 0xff}%02X").mkString + "'::bytea"
    case other => escapeString(other.toString)

  private def escapeString(s: String): String =
    "'" + s.replace("'", "''") + "'"

  def exportTableDataAsJson(connection: Connection, schemaName: String, tableName: String, dialect: String): Json =
    val qualifiedName = dialect match
      case "oracle" => s""""${schemaName.toUpperCase}"."${tableName.toUpperCase}""""
      case _ => s""""$schemaName"."$tableName""""

    val stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    if dialect != "oracle" then stmt.setFetchSize(1000)

    try
      val rs = stmt.executeQuery(s"SELECT * FROM $qualifiedName")
      val meta = rs.getMetaData
      val columnCount = meta.getColumnCount
      val columnNames = (1 to columnCount).map(meta.getColumnName).toSeq
      val columnTypes = (1 to columnCount).map(meta.getColumnType).toArray

      val rows = mutable.ArrayBuffer[Json]()
      while rs.next() do
        val rowObj = columnNames.zipWithIndex.map { case (colName, idx) =>
          val value = valueToJson(rs.getObject(idx + 1), columnTypes(idx), dialect)
          colName -> value
        }
        rows += Json.obj(rowObj*)

      rs.close()

      Json.obj(
        "schema" -> Json.fromString(schemaName),
        "table" -> Json.fromString(tableName),
        "columns" -> columnNames.asJson,
        "rows" -> rows.toSeq.asJson
      )
    finally stmt.close()

  private def valueToJson(value: Any, sqlType: Int, dialect: String): Json = value match
    case null => Json.Null
    case s: String => Json.fromString(s)
    case n: java.lang.Long => Json.fromLong(n)
    case n: java.lang.Integer => Json.fromInt(n)
    case n: java.lang.Double => Json.fromDoubleOrNull(n)
    case n: java.lang.Float => Json.fromFloatOrNull(n)
    case n: java.math.BigDecimal => Json.fromBigDecimal(n)
    case n: java.math.BigInteger => Json.fromBigInt(n)
    case b: java.lang.Boolean => Json.fromBoolean(b)
    case d: java.sql.Date => Json.fromString(d.toString)
    case t: java.sql.Timestamp => Json.fromString(t.toString)
    case t: java.sql.Time => Json.fromString(t.toString)
    case bytes: Array[Byte] => Json.fromString(bytes.map(b => f"${b & 0xff}%02X").mkString)
    case clob: java.sql.Clob =>
      val content = clob.getSubString(1, clob.length().toInt)
      Json.fromString(content)
    case blob: java.sql.Blob =>
      val bytes = blob.getBytes(1, blob.length().toInt)
      Json.fromString(bytes.map(b => f"${b & 0xff}%02X").mkString)
    case other => Json.fromString(other.toString)

  def exportAsInserts(connection: Connection, schemaName: String, tableName: String, dialect: String): Seq[Insert] =
    val qualifiedTableName = dialect match
      case "oracle" => Name(s"${schemaName.toUpperCase}.${tableName.toUpperCase}", quoted = true)
      case _ => Name(s"$schemaName.$tableName", quoted = true)

    val stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    if dialect != "oracle" then stmt.setFetchSize(1000)

    try
      val rs = stmt.executeQuery(s"SELECT * FROM ${renderTableName(schemaName, tableName, dialect)}")
      val meta = rs.getMetaData
      val columnCount = meta.getColumnCount
      val columnNames = (1 to columnCount).map(i => Name(meta.getColumnName(i))).toSeq
      val columnTypes = (1 to columnCount).map(meta.getColumnType).toArray

      val inserts = mutable.ArrayBuffer[Insert]()
      while rs.next() do
        val values = (1 to columnCount).map { i =>
          valueToExpression(rs.getObject(i), columnTypes(i - 1), dialect)
        }
        inserts += Insert(qualifiedTableName, Some(columnNames), ExplicitInsert(Seq(values)))

      rs.close()
      inserts.toSeq
    finally stmt.close()

  private def renderTableName(schemaName: String, tableName: String, dialect: String): String =
    dialect match
      case "oracle" => s""""${schemaName.toUpperCase}"."${tableName.toUpperCase}""""
      case _ => s""""$schemaName"."$tableName""""

  private def valueToExpression(value: Any, sqlType: Int, dialect: String): Expression = value match
    case null => NullPrimitive()
    case s: String => StringPrimitive(s)
    case n: java.lang.Long => LongPrimitive(n)
    case n: java.lang.Integer => LongPrimitive(n.toLong)
    case n: java.lang.Short => LongPrimitive(n.toLong)
    case n: java.lang.Byte => LongPrimitive(n.toLong)
    case n: java.lang.Double => DoublePrimitive(n)
    case n: java.lang.Float => DoublePrimitive(n.toDouble)
    case n: java.math.BigDecimal => DoublePrimitive(n.doubleValue)
    case n: java.math.BigInteger => LongPrimitive(n.longValue)
    case b: java.lang.Boolean => LongPrimitive(if b then 1L else 0L)
    case d: java.sql.Date => StringPrimitive(d.toString)
    case t: java.sql.Timestamp => StringPrimitive(t.toString)
    case t: java.sql.Time => StringPrimitive(t.toString)
    case bytes: Array[Byte] => StringPrimitive(bytes.map(b => f"${b & 0xff}%02X").mkString)
    case clob: java.sql.Clob => StringPrimitive(clob.getSubString(1, clob.length().toInt))
    case blob: java.sql.Blob =>
      StringPrimitive(blob.getBytes(1, blob.length().toInt).map(b => f"${b & 0xff}%02X").mkString)
    case other => StringPrimitive(other.toString)
