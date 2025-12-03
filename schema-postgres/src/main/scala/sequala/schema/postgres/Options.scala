package sequala.schema.postgres

import sequala.schema.{
  ColumnOptions,
  CommonCreateViewOptions,
  CommonDiffOptions,
  CommonDropOptions,
  CommonDropViewOptions,
  CommonExplainOptions,
  CommonIndexOptions,
  DbDialect,
  TableOptions
}

trait PostgresDialect extends DbDialect:
  type DataType = PostgresDataType
  type ColumnOptions = PostgresColumnOptions
  type TableOptions = PostgresTableOptions
  type DropOptions = CommonDropOptions
  type CreateViewOptions = CommonCreateViewOptions
  type DropViewOptions = CommonDropViewOptions
  type IndexOptions = CommonIndexOptions
  type ExplainOptions = CommonExplainOptions
  type DiffOptions = CommonDiffOptions

object PostgresDialect extends PostgresDialect

case class PostgresTableOptions(
  inherits: Seq[String] = Seq.empty,
  partitionBy: Option[PostgresPartitionSpec] = None,
  using: Option[String] = None,
  withOptions: Map[String, String] = Map.empty
) extends TableOptions

object PostgresTableOptions:
  val empty: PostgresTableOptions = PostgresTableOptions()

case class PostgresPartitionSpec(strategy: PostgresPartitionStrategy, columns: Seq[String])

sealed trait PostgresPartitionStrategy
case object PostgresPartitionByRange extends PostgresPartitionStrategy
case object PostgresPartitionByList extends PostgresPartitionStrategy
case object PostgresPartitionByHash extends PostgresPartitionStrategy

case class PostgresColumnOptions(
  generatedAs: Option[PostgresGeneratedColumn] = None,
  compression: Option[String] = None
) extends ColumnOptions

object PostgresColumnOptions:
  val empty: PostgresColumnOptions = PostgresColumnOptions()

case class PostgresGeneratedColumn(expression: String, stored: Boolean = true)
