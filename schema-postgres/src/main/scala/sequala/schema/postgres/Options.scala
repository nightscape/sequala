package sequala.schema.postgres

import sequala.schema.{ColumnOptions, TableOptions}

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
