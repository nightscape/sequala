package sequala.schema.oracle

import sequala.schema.{CommonDataType, DataType}

trait OracleSpecificDataType extends DataType

type OracleDataType = CommonDataType | OracleSpecificDataType

case class Varchar2(length: Int, sizeSemantics: SizeSemantics = Bytes) extends OracleSpecificDataType {
  def toSql: String = s"VARCHAR2($length ${sizeSemantics.toSql})"
}
case class NVarchar2(length: Int) extends OracleSpecificDataType {
  def toSql: String = s"NVARCHAR2($length)"
}
case class OracleChar(length: Int, sizeSemantics: SizeSemantics = Bytes) extends OracleSpecificDataType {
  def toSql: String = s"CHAR($length ${sizeSemantics.toSql})"
}
case class NChar(length: Int) extends OracleSpecificDataType {
  def toSql: String = s"NCHAR($length)"
}
case class Number(precision: Option[Int] = None, scale: Option[Int] = None) extends OracleSpecificDataType {
  def toSql: String = (precision, scale) match {
    case (None, None) => "NUMBER"
    case (Some(p), None) => s"NUMBER($p)"
    case (Some(p), Some(s)) => s"NUMBER($p, $s)"
    case (None, Some(s)) => s"NUMBER(*, $s)"
  }
}
case object BinaryFloat extends OracleSpecificDataType {
  def toSql: String = "BINARY_FLOAT"
}
case object BinaryDouble extends OracleSpecificDataType {
  def toSql: String = "BINARY_DOUBLE"
}
case class Raw(length: Int) extends OracleSpecificDataType {
  def toSql: String = s"RAW($length)"
}
case object OracleLong extends OracleSpecificDataType {
  def toSql: String = "LONG"
}
case object LongRaw extends OracleSpecificDataType {
  def toSql: String = "LONG RAW"
}
case object OracleRowid extends OracleSpecificDataType {
  def toSql: String = "ROWID"
}
case class URowid(length: Option[Int] = None) extends OracleSpecificDataType {
  def toSql: String = length.map(l => s"UROWID($l)").getOrElse("UROWID")
}
case object XMLType extends OracleSpecificDataType {
  def toSql: String = "XMLTYPE"
}
case class IntervalYearToMonth(precision: Option[Int] = None) extends OracleSpecificDataType {
  def toSql: String = precision.map(p => s"INTERVAL YEAR($p) TO MONTH").getOrElse("INTERVAL YEAR TO MONTH")
}
case class IntervalDayToSecond(dayPrecision: Option[Int] = None, secondPrecision: Option[Int] = None)
    extends OracleSpecificDataType {
  def toSql: String = (dayPrecision, secondPrecision) match {
    case (None, None) => "INTERVAL DAY TO SECOND"
    case (Some(d), None) => s"INTERVAL DAY($d) TO SECOND"
    case (None, Some(s)) => s"INTERVAL DAY TO SECOND($s)"
    case (Some(d), Some(s)) => s"INTERVAL DAY($d) TO SECOND($s)"
  }
}

sealed trait SizeSemantics {
  def toSql: String
}
case object Bytes extends SizeSemantics {
  def toSql: String = "BYTE"
}
case object Chars extends SizeSemantics {
  def toSql: String = "CHAR"
}
