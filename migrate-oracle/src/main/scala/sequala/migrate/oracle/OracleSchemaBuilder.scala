package sequala.migrate.oracle

import sequala.converter.SchemaBuilder
import sequala.schema.*
import sequala.schema.oracle.*

/** Oracle-specific SchemaBuilder that converts parsed tables to use Oracle-specific types.
  *
  * The parser produces tables with NoColumnOptions/NoTableOptions. This builder converts them to use
  * OracleColumnOptions.empty/OracleTableOptions.empty so they can be properly processed by Oracle-specific migration
  * code.
  */
object OracleSchemaBuilder extends SchemaBuilder with OracleDialect:

  override def fromStatements(statements: Seq[Statement]): Seq[DialectTable] =
    val baseTables = super.fromStatements(statements)
    baseTables.map(convertToOracleTable)

  private def convertToOracleTable(table: Table[?, ?, ?]): DialectTable =
    Table(
      name = table.name,
      columns = table.columns.map(convertColumn),
      primaryKey = table.primaryKey,
      indexes = table.indexes,
      foreignKeys = table.foreignKeys,
      checks = table.checks,
      uniques = table.uniques,
      options = OracleTableOptions.empty,
      comment = table.comment,
      schema = table.schema
    )

  private def convertColumn(col: Column[?, ?]): Column[OracleDataType, OracleColumnOptions] =
    Column(
      name = col.name,
      dataType = convertDataType(col.dataType.asInstanceOf[DataType]),
      nullable = col.nullable,
      default = col.default,
      options = OracleColumnOptions.empty,
      comment = col.comment
    )

  private def convertDataType(dt: DataType): OracleDataType =
    dt match
      case c: CommonDataType => c
      case o: OracleSpecificDataType => o
      case _ => OracleLong // fallback for unknown types
