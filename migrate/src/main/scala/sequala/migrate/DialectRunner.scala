package sequala.migrate

import sequala.schema.*
import java.sql.Connection

/** Trait that encapsulates all dialect-specific migration operations.
  *
  * Each database dialect provides its own implementation, allowing the CLI to work with any dialect without knowing its
  * internals. This follows the Open-Closed Principle: new dialects can be added without modifying existing code.
  */
trait DialectRunner { self: DbDialect =>

  type DialectTable = Table[DataType, ColumnOptions, TableOptions]
  type DialectColumn = Column[DataType, ColumnOptions]
  type DialectCreateTable = CreateTable[DataType, ColumnOptions, TableOptions]
  type DialectAlterTable = AlterTable[DataType, ColumnOptions, TableOptions, AlterTableAction[DataType, ColumnOptions]]

  /** Parse DDL content and extract tables */
  def parseSourceDDL(content: String): Either[String, Seq[DialectTable]]

  /** Inspect database schema and return tables */
  def inspectSchema(connection: Connection, schema: String): Seq[DialectTable]

  /** Compute diff between two sets of tables */
  def diff(from: Seq[DialectTable], to: Seq[DialectTable], diffOptions: DiffOptions): Seq[SchemaDiffOp]

  /** Render a schema diff operation to SQL statements */
  def renderDiff(diff: SchemaDiffOp): Seq[String]

  /** Render a CREATE TABLE statement */
  def renderCreateTable(table: DialectTable, pretty: Boolean): String

  /** Execute migration steps against a database */
  def executeMigration(connection: Connection, diffs: Seq[SchemaDiffOp], mode: TransactionMode): ExecutionResult

  /** Dialect name for display purposes */
  def dialectName: String
}
