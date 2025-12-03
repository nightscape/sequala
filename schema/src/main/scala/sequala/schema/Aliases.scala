package sequala.schema

type GenericDataType = CommonDataType

type GenericColumn = Column[GenericDataType, NoColumnOptions.type]
type GenericTable = Table[GenericDataType, NoColumnOptions.type, NoTableOptions.type]

type CreateGenericTable = CreateTable[GenericDataType, NoColumnOptions.type, NoTableOptions.type]
type AlterGenericTable = AlterTable[GenericDataType, NoColumnOptions.type, NoTableOptions.type, GenericAlterTableAction]
type GenericAlterTableAction = AlterTableAction[GenericDataType, NoColumnOptions.type]
