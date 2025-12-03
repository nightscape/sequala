package sequala.schema.oracle

import sequala.schema.{ColumnOptions, TableOptions}

case class OracleTableOptions(
  tablespace: Option[String] = None,
  pctFree: Option[Int] = None,
  pctUsed: Option[Int] = None,
  iniTrans: Option[Int] = None,
  maxTrans: Option[Int] = None,
  storage: Option[OracleStorageClause] = None,
  logging: Option[Boolean] = None,
  compress: Option[OracleCompressType] = None,
  cache: Option[Boolean] = None,
  parallel: Option[OracleParallelClause] = None,
  rowMovement: Option[Boolean] = None
) extends TableOptions

object OracleTableOptions:
  val empty: OracleTableOptions = OracleTableOptions()

case class OracleStorageClause(
  initial: Option[String] = None,
  next: Option[String] = None,
  minExtents: Option[Int] = None,
  maxExtents: Option[String] = None,
  pctIncrease: Option[Int] = None,
  bufferPool: Option[String] = None
)

sealed trait OracleCompressType
case object OracleCompressBasic extends OracleCompressType
case object OracleCompressAdvanced extends OracleCompressType
case class OracleCompressFor(operation: String) extends OracleCompressType

case class OracleParallelClause(degree: Option[Int] = None)

case class OracleColumnOptions(virtual: Option[String] = None, invisible: Boolean = false) extends ColumnOptions

object OracleColumnOptions:
  val empty: OracleColumnOptions = OracleColumnOptions()
