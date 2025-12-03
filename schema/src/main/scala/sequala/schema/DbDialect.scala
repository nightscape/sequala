package sequala.schema

trait DbDialect:
  type DataType <: sequala.schema.DataType
  type ColumnOptions <: sequala.schema.ColumnOptions
  type TableOptions <: sequala.schema.TableOptions
  type DropOptions <: sequala.schema.DropOptions
  type CreateViewOptions <: sequala.schema.CreateViewOptions
  type DropViewOptions <: sequala.schema.DropViewOptions
  type IndexOptions <: sequala.schema.IndexOptions
  type ExplainOptions <: sequala.schema.ExplainOptions
  type DiffOptions <: sequala.schema.DiffOptions

trait AnsiDialect extends DbDialect:
  type DataType = CommonDataType
  type ColumnOptions = NoColumnOptions.type
  type TableOptions = NoTableOptions.type
  type DropOptions = CommonDropOptions
  type CreateViewOptions = CommonCreateViewOptions
  type DropViewOptions = CommonDropViewOptions
  type IndexOptions = CommonIndexOptions
  type ExplainOptions = CommonExplainOptions
  type DiffOptions = CommonDiffOptions

object AnsiDialect extends AnsiDialect
