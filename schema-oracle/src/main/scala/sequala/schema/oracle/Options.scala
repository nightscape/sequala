package sequala.schema.oracle

import sequala.schema.{ColumnOptions, CommonDiffOptions, CreateViewOptions, DbDialect, TableOptions}
import sequala.schema.ast.Name

trait OracleDialect extends DbDialect:
  type DataType = OracleDataType
  type ColumnOptions = OracleColumnOptions
  type TableOptions = OracleTableOptions
  type DropOptions = OracleDropOptions
  type CreateViewOptions = OracleCreateViewOptions
  type DropViewOptions = OracleDropViewOptions
  type IndexOptions = OracleIndexOptions
  type ExplainOptions = OracleExplainOptions
  type DiffOptions = CommonDiffOptions

object OracleDialect extends OracleDialect

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

case class OracleDropOptions(cascadeConstraints: Boolean = false, purge: Boolean = false)
    extends sequala.schema.DropOptions

object OracleDropOptions:
  val empty: OracleDropOptions = OracleDropOptions()

case class OracleCreateViewOptions(
  materialized: Boolean = false,
  temporary: Boolean = false,
  force: Boolean = false,
  editionable: Boolean = false,
  columnList: Option[Seq[Name]] = None
) extends CreateViewOptions

object OracleCreateViewOptions:
  val empty: OracleCreateViewOptions = OracleCreateViewOptions()

case class OracleDropViewOptions(cascade: Boolean = false) extends sequala.schema.DropViewOptions

object OracleDropViewOptions:
  val empty: OracleDropViewOptions = OracleDropViewOptions()

case class OracleIndexOptions(tablespace: Option[String] = None, where: Option[String] = None)
    extends sequala.schema.IndexOptions

object OracleIndexOptions:
  val empty: OracleIndexOptions = OracleIndexOptions()

case class OracleExplainOptions() extends sequala.schema.ExplainOptions

object OracleExplainOptions:
  val empty: OracleExplainOptions = OracleExplainOptions()
