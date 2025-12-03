package sequala.migrate.postgres

import sequala.converter.SchemaBuilder
import sequala.schema.*
import sequala.schema.postgres.*

/** Postgres-specific SchemaBuilder that converts parsed tables to use Postgres-specific types.
  *
  * The parser produces tables with NoColumnOptions/NoTableOptions. This builder converts them to use
  * PostgresColumnOptions.empty/PostgresTableOptions.empty so they can be properly processed by Postgres-specific
  * migration code.
  */
object PostgresSchemaBuilder extends SchemaBuilder with PostgresDialect:

  override def fromStatements(statements: Seq[Statement]): Seq[DialectTable] =
    val baseTables = super.fromStatements(statements)
    baseTables.map(convertToPostgresTable)

  private def convertToPostgresTable(table: Table[?, ?, ?]): DialectTable =
    Table(
      name = table.name,
      columns = table.columns.map(convertColumn),
      primaryKey = table.primaryKey,
      indexes = table.indexes,
      foreignKeys = table.foreignKeys,
      checks = table.checks,
      uniques = table.uniques,
      options = PostgresTableOptions.empty,
      comment = table.comment,
      schema = table.schema
    )

  private def convertColumn(col: Column[?, ?]): Column[PostgresDataType, PostgresColumnOptions] =
    Column(
      name = col.name,
      dataType = convertDataType(col.dataType.asInstanceOf[DataType]),
      nullable = col.nullable,
      default = col.default,
      options = PostgresColumnOptions.empty,
      comment = col.comment
    )

  private def convertDataType(dt: DataType): PostgresDataType =
    dt match
      case c: CommonDataType => c
      case p: PostgresSpecificDataType => p
      case _ => SqlText // fallback for unknown types
