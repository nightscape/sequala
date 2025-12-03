package sequala.oracle

// Re-export all Oracle statement types from schema-oracle
export sequala.schema.oracle.{
  AlterTableAdd,
  AlterTableModify,
  ColumnModification,
  Commit,
  CreateOracleView,
  Grant,
  OracleAlterTableAction,
  OracleAlterTableSchema,
  OracleAlterView,
  OracleColumnComment,
  OracleCreateIndexSchema,
  OracleCreateSynonym,
  OracleCreateTable,
  OracleCreateViewOptions,
  OracleDropSynonym,
  OracleDropTableSchema,
  OracleDropViewOptions,
  OracleDropViewSchema,
  OracleExplainOptions,
  OracleIndexOptions,
  OracleRename,
  OracleStatement,
  OracleTableComment,
  ParserAlterTableAction,
  PlSqlBlock,
  PlSqlForLoop,
  PlSqlWhileLoop,
  Prompt,
  Revoke,
  SetDefineOff,
  SetScanOff,
  SetScanOn,
  StorageClause
}

// Aliases for backwards compatibility with old names
type Comment = sequala.schema.oracle.OracleColumnComment
val Comment = sequala.schema.oracle.OracleColumnComment

type TableComment = sequala.schema.oracle.OracleTableComment
val TableComment = sequala.schema.oracle.OracleTableComment
