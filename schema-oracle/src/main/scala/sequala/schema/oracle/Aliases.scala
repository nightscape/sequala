package sequala.schema.oracle

import sequala.schema.{AlterTable, AlterTableAction, Column, CreateTable, CreateView, Explain, Table}

type OracleColumn = Column[OracleDataType, OracleColumnOptions]
type OracleTable = Table[OracleDataType, OracleColumnOptions, OracleTableOptions]
type CreateOracleTable = CreateTable[OracleDataType, OracleColumnOptions, OracleTableOptions]
type CreateOracleView = CreateView[OracleCreateViewOptions]
type AlterOracleTable = AlterTable[OracleDataType, OracleColumnOptions, OracleTableOptions, OracleAlterTableAction]
type OracleAlterTableAction = AlterTableAction[OracleDataType, OracleColumnOptions]
type OracleExplainSchema = Explain[OracleExplainOptions]
