# Merge DDL and Metadata into Unified PROD Tables
#
# This jq script processes all SQL files from multiple schema directories and:
# 1. Extracts CREATE TABLE statements (with actual column types)
# 2. Extracts COMMENT ON TABLE/COLUMN statements
# 3. Extracts metadata from XMDM_CONF_TABLE/COLUMN INSERTs
# 4. Groups output by schema (extracted from file path)
# 5. Filters out _AT/_HT tables, system columns, etc.
# 6. Embeds metadata as -- @KEY: value comments in sourceComment
# 7. Outputs DDL (.sql) files per schema with embedded metadata
#
# Usage:
#   sequala parse oracle \
#     --output 'jq-file-sql:sql/oracle/merge-ddl-and-metadata.jq' \
#     --write-to 'sql/oracle/DESIRED_STATE/' \
#     'sql/oracle/*/PROD/**/*.sql' 'sql/oracle/*/*.sql'
#
# @write-map ddl {key}-PROD-tables.sql
#
# Output Structure:
#   DESIRED_STATE/{SCHEMA}-PROD-tables.sql   - CREATE TABLE with embedded metadata comments

# =====================================================
# Structure Validation - fail early if JSON schema changed
# =====================================================
def validate_structure:
  if .fileResults == null then
    error("SCHEMA MISMATCH: Expected .fileResults array but got null. JSON structure may have changed.")
  elif (.fileResults | type) != "array" then
    error("SCHEMA MISMATCH: Expected .fileResults to be array but got \(.fileResults | type)")
  else . end;

# =====================================================
# sourceComment Helpers
# =====================================================

# Helper to build sourceComment array from metadata map
# Input: { "KEY1": "value1", "KEY2": value2, ... }
# Output: [ { "type": "line", "text": "@KEY1: value1" }, ... ]
# Multi-line values are split into multiple line comments:
#   -- @KEY:
#   --   line 1
#   --   line 2
def build_source_comments:
  [to_entries[] | select(.value != null and .value != "") |
   (.value | tostring) as $val |
   if ($val | contains("\n")) then
     [{ type: "line", text: ("@" + .key + ":") }] +
     [$val | split("\n")[] | { type: "line", text: ("  " + .) }]
   else
     [{ type: "line", text: ("@" + .key + ": " + $val) }]
   end
  ] | flatten;

# Helper to create Name object
def mkname($n): { name: $n, quoted: false };

# Helper to extract value from INSERT by column name
def get_value($columns; $values; $col_name):
  ($columns | to_entries | map(select(.value.name == $col_name)) | .[0].key) as $idx |
  if $idx != null then $values[$idx] else null end;

# Helper to get string value from INSERT
def str_val($columns; $values; $col_name):
  get_value($columns; $values; $col_name) |
  if . != null and .type == "StringPrimitive" then .value else null end;

# Helper to get number value from INSERT
def num_val($columns; $values; $col_name):
  get_value($columns; $values; $col_name) |
  if . != null and .type == "LongPrimitive" then .value
  elif . != null and .type == "StringPrimitive" then (.value | tonumber? // null)
  else null end;

# Build column metadata object for YAML output
def build_column_metadata($col):
  {}
  | if $col.gui_name != null and $col.gui_name != "" then . + { COL_GUI_NAME: $col.gui_name } else . end
  | if $col.desc != null and $col.desc != "" then . + { COL_DESC: $col.desc } else . end
  | if $col.order != null then . + { COL_DISPLAY_ORDER: $col.order } else . end
  | if $col.pk == "Y" then . + { COL_PK: "Y" } else . end
  | if $col.required != null and $col.required != "Y" and $col.required != "N" then . + { COL_REQUIRED: $col.required } else . end
  | if $col.format != null and $col.format != "" then . + { COL_FORMAT: $col.format } else . end
  | if $col.validator != null and $col.validator != "" then . + { COL_VALIDATOR: $col.validator } else . end
  | if $col.multiline != null and $col.multiline != "" then . + { COL_MULTILINE: $col.multiline } else . end
  | if $col.valid_interval != null and $col.valid_interval != "" then . + { COL_VALID_INTERVAL_COLUMN: $col.valid_interval } else . end
  | if $col.lookup_sql != null and $col.lookup_sql != "" then . + { LKP_SQL_STMT1: $col.lookup_sql } else . end
  | if $col.lookup_label != null and $col.lookup_label != "" then . + { LKP_SQL_LABEL: $col.lookup_label } else . end
  | if $col.lookup_value != null and $col.lookup_value != "" then . + { LKP_SQL_VALUE: $col.lookup_value } else . end;

# Extract just the table name (without schema prefix)
def table_name_only:
  if contains(".") then split(".") | .[-1] else . end;

# Extract schema name from qualified name (e.g., "SCHEMA.TABLE" or "SCHEMA.TABLE.COLUMN" -> "SCHEMA")
def extract_schema_from_name:
  if contains(".") then split(".") | .[0] else null end;

# Extract table name from qualified column name (e.g., "SCHEMA.TABLE.COLUMN" -> "TABLE")
def extract_table_from_column_name:
  split(".") |
  if length >= 2 then .[length - 2] else null end;

# Extract schema name from file path (e.g., "sql/oracle/CLOUDERA/..." -> "CLOUDERA")
def extract_schema_from_path:
  split("/") |
  . as $parts |
  ($parts | to_entries | map(select(.value == "oracle")) | .[0].key) as $oracle_idx |
  if $oracle_idx != null and ($oracle_idx + 1) < ($parts | length) then
    $parts[$oracle_idx + 1]
  else
    "UNKNOWN"
  end;

# =====================================================
# Main Processing
# =====================================================

# Validate structure first
validate_structure |

# Collect all statements with their source file info
[.fileResults[] |
  .relativePath as $path |
  ($path | extract_schema_from_path) as $schema |
  .statementResults[] |
  select(.parseResult.success == true) |
  {
    schema: $schema,
    stmt: .parseResult.value
  }
] |

# Separate by statement type and schema
# Note: CreateTable uses .table.name (string), OracleInsert uses .table.name (string in object)
group_by(.schema) | map({
  schema: .[0].schema,
  creates: [.[] | .stmt | select(.type == "CreateTable") | select(.table.name | table_name_only | test("_(AT|HT)$") | not)],
  table_inserts: [.[] | .stmt | select(.type == "Insert" or .type == "OracleInsert") | select(.table.name | test("XMDM_CONF_TABLE$"))],
  column_inserts: [.[] | .stmt | select(.type == "Insert" or .type == "OracleInsert") | select(.table.name | test("XMDM_CONF_COLUMN$"))],
  table_comments: [.[] | .stmt | select(.type == "TableComment")],
  column_comments: [.[] | .stmt | select(.type == "Comment")]
}) |

# Process each schema
[.[] |
  .schema as $schema |

  # Build table metadata map: { "TABLE_NAME": { gui_name, short_name, at_table, order, has_ht } }
  ([ .table_inserts[] |
    .columns as $cols |
    .values.values[0] as $vals |
    (str_val($cols; $vals; "TBL_NAME")) as $tbl_name |
    select($tbl_name != null) |
    {
      key: $tbl_name,
      value: {
        gui_name: str_val($cols; $vals; "TBL_GUI_NAME"),
        short_name: str_val($cols; $vals; "TBL_GUI_NAME_SHORT"),
        at_table: str_val($cols; $vals; "TBL_NAME_AT"),
        order: num_val($cols; $vals; "TBL_DISPLAY_ORDER"),
        has_ht: str_val($cols; $vals; "HISTORICAL_TBL_FLAG")
      }
    }
  ] | from_entries) as $table_meta |

  # Build column metadata map: { "TABLE_NAME.COL_NAME": { gui_name, order, pk, ... } }
  ([ .column_inserts[] |
    .columns as $cols |
    .values.values[0] as $vals |
    (str_val($cols; $vals; "TBL_NAME")) as $tbl_name |
    (str_val($cols; $vals; "COL_NAME")) as $col_name |
    select($tbl_name != null and $col_name != null) |
    {
      key: ($tbl_name + "." + $col_name),
      value: {
        gui_name: str_val($cols; $vals; "COL_GUI_NAME"),
        desc: str_val($cols; $vals; "COL_DESC"),
        order: num_val($cols; $vals; "COL_DISPLAY_ORDER"),
        pk: str_val($cols; $vals; "COL_PK"),
        required: str_val($cols; $vals; "COL_REQUIRED"),
        format: str_val($cols; $vals; "COL_FORMAT"),
        validator: str_val($cols; $vals; "COL_VALIDATOR"),
        multiline: str_val($cols; $vals; "COL_MULTILINE"),
        valid_interval: str_val($cols; $vals; "COL_VALID_INTERVAL_COLUMN"),
        lookup_sql: str_val($cols; $vals; "LKP_SQL_STMT1"),
        lookup_label: str_val($cols; $vals; "LKP_SQL_LABEL"),
        lookup_value: str_val($cols; $vals; "LKP_SQL_VALUE")
      }
    }
  ] | from_entries) as $column_meta |

  # Get unique CREATE TABLE statements (keep last occurrence for each table name)
  (.creates | group_by(.table.name | table_name_only) | map(.[-1])) as $unique_creates |

  # Build set of table names (without schema prefix) for this schema group
  ($unique_creates | map(.table.name | table_name_only) | unique) as $table_names |

  # Filter COMMENT ON statements to match tables in this schema group
  # Match by table name (without schema prefix) to handle cases where schema in qualified name differs from directory schema
  (.table_comments | map(select(.tableNameParsed.name | table_name_only | IN($table_names[])))) as $schema_table_comments |
  (.column_comments | map(select(.qualifiedColumnName.name | extract_table_from_column_name | IN($table_names[])))) as $schema_column_comments |

  # Build DDL statements with embedded metadata in sourceComment
  ([$unique_creates[] |
    . as $create |
    (.table.name | table_name_only) as $tbl_name |
    ($table_meta[$tbl_name] // null) as $tbl_meta |
    ($create.table.columns // []) as $columns |

    # Build column metadata map (excluding system columns)
    ([$columns[] |
      .name as $col_name |
      select($col_name != "ITS" and $col_name != "ACTION" and $col_name != "MODIFIED_BY" and
             $col_name != "MODIFIED_AT" and $col_name != "ACCEPTED_BY" and $col_name != "ACCEPTED_AT" and
             $col_name != "STATUS" and $col_name != "STATUS_AT") |
      ($tbl_name + "." + $col_name) as $col_key |
      ($column_meta[$col_key] // null) as $col_m |
      select($col_m != null) |
      { key: $col_name, value: build_column_metadata($col_m) }
    ] | from_entries) as $columns_metadata |

    # Build table-level metadata for sourceComment
    ({}
      | if $tbl_meta != null and $tbl_meta.gui_name != null then . + { TBL_GUI_NAME: $tbl_meta.gui_name } else . end
      | if $tbl_meta != null and $tbl_meta.short_name != null then . + { TBL_GUI_NAME_SHORT: $tbl_meta.short_name } else . end
      | if $tbl_meta != null and $tbl_meta.at_table != null then . + { TBL_NAME_AT: $tbl_meta.at_table } else . end
      | if $tbl_meta != null and $tbl_meta.order != null then . + { TBL_DISPLAY_ORDER: $tbl_meta.order } else . end
      | if $tbl_meta != null and $tbl_meta.has_ht == "Y" then . + { HISTORICAL_TBL_FLAG: "Y" } else . end
    ) as $table_metadata |

    # Update columns with sourceComment containing column metadata
    ($columns | map(
      . as $col |
      ($columns_metadata[.name] // {}) as $col_meta |
      if ($col_meta | length) > 0 then
        . + { sourceComment: ($col_meta | build_source_comments) }
      else
        .
      end
    )) as $updated_columns |

    # Build the updated CREATE TABLE with table sourceComment and updated columns
    $create |
    .table |= (
      . +
      (if ($table_metadata | length) > 0 then { sourceComment: ($table_metadata | build_source_comments) } else {} end) +
      { columns: $updated_columns }
    )
  ]) as $creates_with_metadata |

  # Only emit schemas that have tables
  select(($unique_creates | length) > 0) |

  {
    schema: $schema,
    # DDL includes tables with embedded metadata plus COMMENT ON statements
    ddl: ($creates_with_metadata + $schema_table_comments + $schema_column_comments)
  }
] |

# Restructure for @write-map: { ddl: { SCHEMA: [...], ... } }
{
  ddl: ([.[] | { key: .schema, value: .ddl }] | from_entries)
}
