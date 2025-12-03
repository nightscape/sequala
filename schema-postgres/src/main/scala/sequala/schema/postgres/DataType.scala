package sequala.schema.postgres

import sequala.schema.{CommonDataType, DataType}

trait PostgresSpecificDataType extends DataType

type PostgresDataType = CommonDataType | PostgresSpecificDataType

case object Serial extends PostgresSpecificDataType {
  def toSql: String = "SERIAL"
}
case object BigSerial extends PostgresSpecificDataType {
  def toSql: String = "BIGSERIAL"
}
case object SmallSerial extends PostgresSpecificDataType {
  def toSql: String = "SMALLSERIAL"
}
case object Uuid extends PostgresSpecificDataType {
  def toSql: String = "UUID"
}
case object Json extends PostgresSpecificDataType {
  def toSql: String = "JSON"
}
case object Jsonb extends PostgresSpecificDataType {
  def toSql: String = "JSONB"
}
case object Inet extends PostgresSpecificDataType {
  def toSql: String = "INET"
}
case object Cidr extends PostgresSpecificDataType {
  def toSql: String = "CIDR"
}
case object MacAddr extends PostgresSpecificDataType {
  def toSql: String = "MACADDR"
}
case object MacAddr8 extends PostgresSpecificDataType {
  def toSql: String = "MACADDR8"
}
case object Money extends PostgresSpecificDataType {
  def toSql: String = "MONEY"
}
case object Bytea extends PostgresSpecificDataType {
  def toSql: String = "BYTEA"
}
case class Bit(length: Option[Int] = None) extends PostgresSpecificDataType {
  def toSql: String = length.map(l => s"BIT($l)").getOrElse("BIT")
}
case class BitVarying(length: Option[Int] = None) extends PostgresSpecificDataType {
  def toSql: String = length.map(l => s"BIT VARYING($l)").getOrElse("BIT VARYING")
}
case object PgPoint extends PostgresSpecificDataType {
  def toSql: String = "POINT"
}
case object PgLine extends PostgresSpecificDataType {
  def toSql: String = "LINE"
}
case object PgBox extends PostgresSpecificDataType {
  def toSql: String = "BOX"
}
case object PgCircle extends PostgresSpecificDataType {
  def toSql: String = "CIRCLE"
}
case object PgPolygon extends PostgresSpecificDataType {
  def toSql: String = "POLYGON"
}
case object PgPath extends PostgresSpecificDataType {
  def toSql: String = "PATH"
}
case object TsVector extends PostgresSpecificDataType {
  def toSql: String = "TSVECTOR"
}
case object TsQuery extends PostgresSpecificDataType {
  def toSql: String = "TSQUERY"
}
case class PgArray[E <: DataType](elementType: E) extends PostgresSpecificDataType {
  def toSql: String = s"${elementType.toSql}[]"
}
