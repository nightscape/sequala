# Filter DDL output from dump command
#
# Filters out _AT (audit) and _HT (history) tables from the DDL export.
# Also filters out COMMENT statements for _AT/_HT tables.
# Groups output by schema and writes to {schema}-PROD-tables.sql files.
#
# Input JSON structure:
# {
#   "tables": [
#     { "schema": "SCHEMA_NAME", "statement": { "type": "CreateTable", "table": {...} } },
#     { "schema": "SCHEMA_NAME", "statement": { "type": "OracleTableComment", ... } },
#     { "schema": "SCHEMA_NAME", "statement": { "type": "OracleColumnComment", ... } }
#   ],
#   "schemas": ["SCHEMA1", "SCHEMA2"]
# }
#
# Usage:
#   sequala dump --ddls "GUI_XMDM%" --ddlsFilter sql/oracle/dump-filter-ddl.jq --outputDir dump/
#
# @write-map ddl {key}-PROD-tables.sql

# Helper to extract table name from different statement types
def get_table_name:
  if .statement.type == "CreateTable" then
    .statement.table.name
  elif .statement.type == "OracleTableComment" then
    # tableNameParsed.name is like "SCHEMA"."TABLE"
    .statement.tableNameParsed.name | split(".") | last | gsub("\""; "")
  elif .statement.type == "OracleColumnComment" then
    # qualifiedColumnName.name is like "SCHEMA"."TABLE"."COLUMN"
    .statement.qualifiedColumnName.name | split(".") | .[-2] | gsub("\""; "")
  else
    ""
  end;

# Filter out statements for _AT and _HT tables
.tables
| map(select(get_table_name | test("_(AT|HT)$") | not))

# Group by schema
| group_by(.schema)

# Convert to object with schema as key, array of statements as value
| map({
    key: .[0].schema,
    value: [.[].statement]
  })
| from_entries

# Wrap in ddl key for @write-map
| {ddl: .}
