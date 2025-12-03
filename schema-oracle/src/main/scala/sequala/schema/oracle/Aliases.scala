package sequala.schema.oracle

import sequala.schema.{AlterTable, AlterTableAction, Column, CreateTable, Table}

type OracleColumn = Column[OracleDataType, OracleColumnOptions]
type OracleTable = Table[OracleDataType, OracleColumnOptions, OracleTableOptions]
type CreateOracleTable = CreateTable[OracleDataType, OracleColumnOptions, OracleTableOptions]
type AlterOracleTable = AlterTable[OracleDataType, OracleColumnOptions, OracleTableOptions, OracleAlterTableAction]
type OracleAlterTableAction = AlterTableAction[OracleDataType, OracleColumnOptions]
