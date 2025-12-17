# Unified filter for DDL and metadata from database dump
#
# Combines DDL filtering and metadata transformation in a single pass,
# enabling correlation between DDL tables and metadata entries.
# Filters out _AT (audit) and _HT (history) tables from DDL output.
# Embeds XMDM_CONF_TABLE/COLUMN metadata as -- @KEY: value comments in sourceComment.
# Auto-generates column overrides for _AT/_HT tables by comparing actual vs expected.
# Groups output by schema and writes to {schema}-PROD-tables.sql files.
#
# Input JSON structure:
# {
#   "ddl": {
#     "tables": [
#       { "schema": "SCHEMA_NAME", "table": "TABLE_NAME", "statement": { "type": "CreateTable", "table": {...} } },
#       { "schema": "SCHEMA_NAME", "table": "TABLE_NAME", "statement": { "type": "OracleTableComment", ... } },
#       { "schema": "SCHEMA_NAME", "table": "TABLE_NAME", "statement": { "type": "OracleColumnComment", ... } }
#     ],
#     "schemas": ["SCHEMA1", "SCHEMA2"]
#   },
#   "data": {
#     "tables": [
#       {
#         "schema": "GUI_XMDM",
#         "table": "XMDM_CONF_TABLE",
#         "data": {
#           "columns": ["TBL_NAME", "TBL_SCHEMA", "TBL_GUI_NAME", ...],
#           "rows": [{"TBL_NAME": "...", "TBL_SCHEMA": "GUI_XMDM_F7", ...}]
#         }
#       }
#     ],
#     "schemas": ["GUI_XMDM"]
#   }
# }
#
# Usage:
#   sequala dump \
#     --ddls "GUI_XMDM%" \
#     --data "GUI_XMDM.XMDM_CONF%" \
#     --filter sql/oracle/unified-dump-filter.jq \
#     --outputDir DESIRED_STATE/
#
# @write-map ddl {key}-PROD-tables.sql
# @write setup _setup.sql

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

# Helper to build column metadata object (now returns metadata map for sourceComment)
def build_column_metadata($col):
  {}
  | if $col.COL_GUI_NAME != null and $col.COL_GUI_NAME != "" then . + { COL_GUI_NAME: $col.COL_GUI_NAME } else . end
  | if $col.COL_DESC != null and $col.COL_DESC != "" then . + { COL_DESC: $col.COL_DESC } else . end
  | if $col.COL_DISPLAY_ORDER != null then . + { COL_DISPLAY_ORDER: ($col.COL_DISPLAY_ORDER | tonumber? // $col.COL_DISPLAY_ORDER) } else . end
  | if $col.COL_PK == "Y" then . + { COL_PK: "Y" } else . end
  | if $col.COL_REQUIRED != null and $col.COL_REQUIRED != "Y" and $col.COL_REQUIRED != "N" then . + { COL_REQUIRED: $col.COL_REQUIRED } else . end
  | if $col.COL_FORMAT != null and $col.COL_FORMAT != "" then . + { COL_FORMAT: $col.COL_FORMAT } else . end
  | if $col.COL_VALIDATOR != null and $col.COL_VALIDATOR != "" then . + { COL_VALIDATOR: $col.COL_VALIDATOR } else . end
  | if $col.COL_MULTILINE != null and $col.COL_MULTILINE != "" then . + { COL_MULTILINE: $col.COL_MULTILINE } else . end
  | if $col.COL_VALID_INTERVAL_COLUMN != null and $col.COL_VALID_INTERVAL_COLUMN != "" then . + { COL_VALID_INTERVAL_COLUMN: $col.COL_VALID_INTERVAL_COLUMN } else . end
  | if $col.LKP_SQL_STMT1 != null and $col.LKP_SQL_STMT1 != "" then . + { LKP_SQL_STMT1: $col.LKP_SQL_STMT1 } else . end
  | if $col.LKP_SQL_LABEL != null and $col.LKP_SQL_LABEL != "" then . + { LKP_SQL_LABEL: $col.LKP_SQL_LABEL } else . end
  | if $col.LKP_SQL_VALUE != null and $col.LKP_SQL_VALUE != "" then . + { LKP_SQL_VALUE: $col.LKP_SQL_VALUE } else . end;

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

# Helper to check if table name ends with _AT or _HT
def is_base_table:
  test("_(AT|HT)$") | not;

# Check if column name is an audit column
def is_audit_column:
  . == "ACTION" or . == "MODIFIED_BY" or . == "MODIFIED_AT" or
  . == "ACCEPTED_BY" or . == "ACCEPTED_AT" or . == "STATUS" or . == "STATUS_AT";

# Standard audit columns that _AT tables should have
def audit_column_names:
  ["ACTION", "MODIFIED_BY", "MODIFIED_AT", "ACCEPTED_BY", "ACCEPTED_AT", "STATUS", "STATUS_AT"];

# Compute expected _AT column names from base table columns
# Returns array of column names in expected order
def expected_at_columns($base_columns):
  ($base_columns | map(select(.name | is_audit_column | not))) as $non_audit |
  ($non_audit | map(.name)) as $base_names |
  ($non_audit | map(select(.name == "ITS")) | length > 0) as $has_its |
  if $has_its then
    # ITS exists: base columns (without ITS) + audit columns + ITS
    ([$non_audit[] | select(.name != "ITS") | .name]) + audit_column_names + ["ITS"]
  else
    # No ITS: base columns + audit columns
    $base_names + audit_column_names
  end;

# Compute expected _HT column names from base table columns
def expected_ht_columns($base_columns):
  [$base_columns[] | select(.name | is_audit_column | not) | .name];

# Compare actual column with expected and return override if different
# $actual: actual column from DB
# $expected: expected column (from base table or standard audit column)
# Returns: override object or null if no differences
def compute_column_override($actual; $expected):
  if $expected == null then
    # Column exists in actual but not expected - it's an addition
    # For ITS, just mark presence; for others, include dataType
    if $actual.name == "ITS" then
      {}
    else
      { dataType: $actual.dataType }
    end
  else
    # Column exists in both - check for differences
    # Note: Cannot use // operator for nullable because `false // true` returns true in jq
    (($actual.nullable | if . == null then true else . end)) as $actual_nullable |
    (($expected.nullable | if . == null then true else . end)) as $expected_nullable |
    # Compare data types - normalize by removing null fields for comparison
    ($actual.dataType | del(.[] | select(. == null))) as $actual_type |
    ($expected.dataType | del(.[] | select(. == null))) as $expected_type |
    (
      {}
      | if $actual_nullable != $expected_nullable then
          . + { nullable: $actual_nullable }
        else . end
      | if $actual_type != $expected_type then
          . + { dataType: $actual.dataType }
        else . end
    ) as $diff |
    if ($diff | length) > 0 then $diff else null end
  end;

# Build column overrides for an _AT table by comparing actual vs expected
# $actual_columns: columns from actual _AT table in DB
# $base_columns: columns from base table
# Returns: { "COL_NAME": { override }, ... } or empty object if no overrides needed
def build_at_overrides($actual_columns; $base_columns):
  expected_at_columns($base_columns) as $expected_names |
  # Build map of actual columns by name
  ([$actual_columns[] | {key: .name, value: .}] | from_entries) as $actual_col_map |
  # Build map of base columns (non-audit)
  ([$base_columns[] | select(.name | is_audit_column | not) | {key: .name, value: .}] | from_entries) as $base_col_map |

  # Check each actual column for differences
  ([
    $actual_columns[] |
    .name as $col_name |
    . as $actual_col |
    # Is this column expected?
    ($col_name | IN($expected_names[])) as $is_expected |
    # Is this an audit column or ITS?
    ($col_name | is_audit_column) as $is_audit |
    ($col_name == "ITS") as $is_its |

    if $is_expected then
      if $is_audit or $is_its then
        # Audit columns and ITS are expected with standard types - no override needed
        empty
      else
        # Base column - check for nullable differences
        ($base_col_map[$col_name] // null) as $expected_col |
        if $expected_col != null then
          compute_column_override($actual_col; $expected_col) as $override |
          if $override != null then
            { key: $col_name, value: $override }
          else
            empty
          end
        else
          empty
        end
      end
    else
      # Column not expected - it's an addition
      if $col_name == "ITS" then
        # ITS added when base doesn't have it - just mark presence
        { key: $col_name, value: {} }
      else
        # Other unexpected column - include dataType
        { key: $col_name, value: { dataType: $actual_col.dataType } }
      end
    end
  ] | from_entries) as $existing_overrides |

  # Check for expected columns that are MISSING from actual _AT table
  ([
    $expected_names[] |
    . as $expected_name |
    # Skip if column exists in actual
    select($actual_col_map[$expected_name] == null) |
    # Mark as skipped
    { key: $expected_name, value: { skip: true } }
  ] | from_entries) as $skip_overrides |

  # Merge both override sets
  $existing_overrides + $skip_overrides;

# Capture setup info from input before processing
(.setup // {}) as $setup_info |

# Extract table data from XMDM_CONF_TABLE
(.data.tables | map(select(.table | test("XMDM_CONF_TABLE$"))) | map(.data.rows) | add // []) as $table_rows |

# Extract column data from XMDM_CONF_COLUMN
(.data.tables | map(select(.table | test("XMDM_CONF_COLUMN$"))) | map(.data.rows) | add // []) as $column_rows |

# Build table metadata map: { "TABLE_NAME": { TBL_SCHEMA, gui_name, ... } }
([$table_rows[] |
  select(.TBL_NAME != null) |
  {
    key: .TBL_NAME,
    value: {
      schema: (.TBL_SCHEMA // "UNKNOWN"),
      gui_name: .TBL_GUI_NAME,
      short_name: .TBL_GUI_NAME_SHORT,
      at_table: .TBL_NAME_AT,
      order: (.TBL_DISPLAY_ORDER | tonumber? // null),
      has_ht: .HISTORICAL_TBL_FLAG
    }
  }
] | from_entries) as $table_meta |

# Build column metadata map: { "TABLE_NAME.COL_NAME": { ... } }
([$column_rows[] |
  select(.TBL_NAME != null and .COL_NAME != null) |
  {
    key: (.TBL_NAME + "." + .COL_NAME),
    value: .
  }
] | from_entries) as $column_meta |

# Build map of ALL tables (including _AT/_HT) with their columns for override computation
# { "SCHEMA.TABLE": { columns: [...], ... }, ... }
([.ddl.tables[] |
  select(.statement.type == "CreateTable") |
  { key: (.schema + "." + .table), value: .statement.table }
] | from_entries) as $all_tables_map |

# Process DDL tables - filter _AT/_HT for output, but use all for override computation
.ddl.tables
| map(select(.table | is_base_table))
| group_by(.schema)
| map({
    schema: .[0].schema,
    tables: .
  })
| map(
    .schema as $schema |
    .tables as $tables |

    # Build column metadata map for each table: { "TABLE_NAME": { "COL_NAME": {...}, ... }, ... }
    ([$tables[] |
      select(.statement.type == "CreateTable") |
      .table as $tbl_name |
      ([$column_meta | to_entries[] | select(.key | startswith($tbl_name + ".")) |
        {key: (.key | split(".")[1]), value: build_column_metadata(.value)}
      ] | from_entries) as $cols_meta |
      { key: $tbl_name, value: $cols_meta }
    ] | from_entries) as $table_columns_meta |

    # Build DDL statements with embedded metadata in sourceComment
    ([$tables[] |
      if .statement.type == "CreateTable" then
        .table as $tbl_name |
        ($table_meta[$tbl_name] // null) as $tbl_meta |
        ($table_columns_meta[$tbl_name] // {}) as $cols_meta |

        # Build table-level metadata for sourceComment
        ({}
          | if $tbl_meta != null and $tbl_meta.gui_name != null then . + { TBL_GUI_NAME: $tbl_meta.gui_name } else . end
          | if $tbl_meta != null and $tbl_meta.short_name != null then . + { TBL_GUI_NAME_SHORT: $tbl_meta.short_name } else . end
          | if $tbl_meta != null and $tbl_meta.at_table != null then . + { TBL_NAME_AT: $tbl_meta.at_table } else . end
          | if $tbl_meta != null and $tbl_meta.order != null then . + { TBL_DISPLAY_ORDER: $tbl_meta.order } else . end
          | if $tbl_meta != null and $tbl_meta.has_ht == "Y" then . + { HISTORICAL_TBL_FLAG: "Y" } else . end
        ) as $table_metadata |

        # Update columns with sourceComment containing column metadata
        (.statement.table.columns | map(
          . as $col |
          ($cols_meta[.name] // {}) as $col_meta |
          if ($col_meta | length) > 0 then
            . + { sourceComment: ($col_meta | build_source_comments) }
          else
            .
          end
        )) as $updated_columns |

        # Build the updated statement with table sourceComment and updated columns
        .statement |
        .table |= (
          . +
          (if ($table_metadata | length) > 0 then { sourceComment: ($table_metadata | build_source_comments) } else {} end) +
          { columns: $updated_columns }
        )
      else
        # Pass through non-CreateTable statements as-is
        .statement
      end
    ]) as $ddl_statements |

    # Compute _AT/_HT overrides for each base table (for override comments)
    ([$tables[] |
      select(.statement.type == "CreateTable") |
      .table as $tbl_name |
      .statement.table.columns as $base_columns |
      ($table_meta[$tbl_name] // {}) as $tbl_meta |

      # Get actual _AT table name (from metadata or default)
      ($tbl_meta.at_table // ($tbl_name + "_AT")) as $at_table_name |

      # Look up actual _AT table from DDL
      ($all_tables_map[$schema + "." + $at_table_name] // null) as $actual_at |

      # If _AT exists in DB, compute overrides and emit as comment
      if $actual_at != null then
        build_at_overrides($actual_at.columns; $base_columns) as $at_overrides |
        if ($at_overrides | length) > 0 then
          # Create a comment statement documenting the _AT overrides
          { key: $at_table_name, value: $at_overrides }
        else
          empty
        end
      else
        empty
      end,

      # Same for _HT if it exists
      ($tbl_name + "_HT") as $ht_table_name |
      ($all_tables_map[$schema + "." + $ht_table_name] // null) as $actual_ht |
      if $actual_ht != null then
        # For _HT, expected columns are just base columns (no audit)
        expected_ht_columns($base_columns) as $expected_ht_names |
        ([
          $actual_ht.columns[] |
          .name as $col_name |
          . as $actual_col |
          ($col_name | IN($expected_ht_names[])) as $is_expected |
          if $is_expected | not then
            if $col_name == "ITS" then
              { key: $col_name, value: {} }
            else
              { key: $col_name, value: { dataType: $actual_col.dataType } }
            end
          else
            empty
          end
        ] | from_entries) as $ht_overrides |
        if ($ht_overrides | length) > 0 then
          { key: $ht_table_name, value: $ht_overrides }
        else
          empty
        end
      else
        empty
      end
    ] | from_entries) as $override_map |

    # Include schema if it has DDL
    select(($ddl_statements | length) > 0) |
    { key: $schema, ddl: $ddl_statements, overrides: $override_map }
  )

# Restructure for @write-map directive (DDL only, metadata embedded in sourceComment)
# Also include setup statements if present
| {
    # Setup: combine tablespace and user statements into a single array for rendering as SQL
    setup: (($setup_info.tablespaces // []) + ($setup_info.users // [])),
    ddl: ([.[] | { key: .key, value: .ddl }] | from_entries)
  }
