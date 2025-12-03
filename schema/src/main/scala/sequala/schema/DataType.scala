package sequala.schema

trait DataType {
  def toSql: String
}

sealed trait CommonDataType extends DataType

case class VarChar(length: Int) extends CommonDataType {
  def toSql: String = s"VARCHAR($length)"
}
case class SqlChar(length: Int) extends CommonDataType {
  def toSql: String = s"CHAR($length)"
}
case object SqlInteger extends CommonDataType {
  def toSql: String = "INTEGER"
}
case object SqlBigInt extends CommonDataType {
  def toSql: String = "BIGINT"
}
case object SmallInt extends CommonDataType {
  def toSql: String = "SMALLINT"
}
case class Decimal(precision: Int, scale: Int) extends CommonDataType {
  def toSql: String = s"DECIMAL($precision, $scale)"
}
case class Numeric(precision: Int, scale: Int) extends CommonDataType {
  def toSql: String = s"NUMERIC($precision, $scale)"
}
case object Real extends CommonDataType {
  def toSql: String = "REAL"
}
case object DoublePrecision extends CommonDataType {
  def toSql: String = "DOUBLE PRECISION"
}
case object SqlBoolean extends CommonDataType {
  def toSql: String = "BOOLEAN"
}
case object SqlDate extends CommonDataType {
  def toSql: String = "DATE"
}
case class SqlTime(precision: Option[Int] = None, withTimeZone: Boolean = false) extends CommonDataType {
  def toSql: String = {
    val base = precision.map(p => s"TIME($p)").getOrElse("TIME")
    if withTimeZone then s"$base WITH TIME ZONE" else base
  }
}
case class SqlTimestamp(precision: Option[Int] = None, withTimeZone: Boolean = false) extends CommonDataType {
  def toSql: String = {
    val base = precision.map(p => s"TIMESTAMP($p)").getOrElse("TIMESTAMP")
    if withTimeZone then s"$base WITH TIME ZONE" else base
  }
}
case object SqlText extends CommonDataType {
  def toSql: String = "TEXT"
}
case object SqlBlob extends CommonDataType {
  def toSql: String = "BLOB"
}
case object SqlClob extends CommonDataType {
  def toSql: String = "CLOB"
}
