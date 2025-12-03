package sequala.schema.postgres

import sequala.schema.{AlterTable, AlterTableAction, Column, CreateTable, Table}

type PostgresColumn = Column[PostgresDataType, PostgresColumnOptions]
type PostgresTable = Table[PostgresDataType, PostgresColumnOptions, PostgresTableOptions]
type CreatePostgresTable = CreateTable[PostgresDataType, PostgresColumnOptions, PostgresTableOptions]
type AlterPostgresTable =
  AlterTable[PostgresDataType, PostgresColumnOptions, PostgresTableOptions, PostgresAlterTableAction]
type PostgresAlterTableAction = AlterTableAction[PostgresDataType, PostgresColumnOptions]
