# DDL to Metadata Transformation
# Converts CreateTable and AlterTable statements to metadata INSERTs/UPDATEs/DELETEs
#
# Usage with jq-sql to get SQL output:
#   sequala parse oracle --output 'jq-file-sql:sql/oracle/ddl-to-metadata.jq' <ddl-file.sql>
#
# Handles:
#   - CREATE TABLE -> INSERT into XMDM_CONF_TABLE + XMDM_CONF_COLUMN
#   - ALTER TABLE ADD -> INSERT into XMDM_CONF_COLUMN
#   - ALTER TABLE MODIFY -> UPDATE XMDM_CONF_COLUMN (datatype, required)
#   - ALTER TABLE DROP COLUMN -> DELETE from XMDM_CONF_COLUMN
#   - ALTER TABLE RENAME COLUMN -> UPDATE XMDM_CONF_COLUMN
#
# =============================================================================
# Metadata from sourceComment
# =============================================================================
# Metadata is now read from -- @KEY: value comments in the DDL's sourceComment field
# on tables and columns, instead of external YAML files.
#
# Table-level comments (before CREATE TABLE):
#   -- @TBL_GUI_NAME: Price Trade Monitor
#   -- @TBL_GUI_NAME_SHORT: PRICE_TRADE_MONITOR
#   -- @TBL_NAME_AT: PRICE_TRADE_MONITOR_AT
#   -- @TBL_DISPLAY_ORDER: 4
#
# Column-level comments (before each column):
#   -- @COL_GUI_NAME: Display Name
#   -- @COL_DISPLAY_ORDER: 1
#   -- @COL_PK: Y
#   -- @COL_DESC: Description
#   -- @LKP_SQL_STMT1: SELECT ... (multi-line with \)
#
# =============================================================================
# Notes
# =============================================================================
# - System columns (ITS, ACTION, MODIFIED_BY, etc.) are automatically filtered out
# - APP_NAME uses placeholder '{{APP_NAME}}' - replace when deploying
# - Tables ending with _AT are excluded (audit trail tables)
# - Default values are derived from DDL when metadata is not provided

# =============================================================================
# sourceComment parsing helpers
# =============================================================================

# Parse @KEY: value from sourceComment array (with continuation support)
# Input: sourceComment array [ { type: "line", text: "..." }, ... ]
# Output: { "KEY1": "value1", "KEY2": "value2", ... }
def parse_source_comments:
  if . == null or (. | length) == 0 then {}
  else
    # Join line comment texts, handling continuation (lines ending with \)
    [.[] | select(.type == "line") | .text] |
    reduce .[] as $line (
      { result: [], current: null };
      if .current != null then
        # Previous line had continuation
        if $line | test("\\\\$") then
          { result: .result, current: (.current + " " + ($line | rtrimstr("\\"))) }
        else
          { result: (.result + [.current + " " + $line]), current: null }
        end
      elif $line | test("^@[A-Z_0-9]+:") then
        if $line | test("\\\\$") then
          { result: .result, current: ($line | rtrimstr("\\")) }
        else
          { result: (.result + [$line]), current: null }
        end
      else
        .
      end
    ) |
    # Don't forget to flush any remaining current line
    (if .current != null then .result + [.current] else .result end) |
    # Parse @KEY: value
    [.[] | capture("^@(?<key>[A-Z_0-9]+):\\s*(?<value>.*)$") // empty] |
    [.[] | {key: .key, value: .value}] |
    from_entries
  end;

# Helper function to determine COL_DATATYPE from Oracle type name
def datatype_code:
  if test("^VARCHAR"; "i") then "S"
  elif test("^CHAR"; "i") then "S"
  elif test("^NVARCHAR"; "i") then "S"
  elif test("^NUMBER"; "i") then "N"
  elif test("^FLOAT"; "i") then "N"
  elif test("^INTEGER"; "i") then "N"
  elif test("^DATE"; "i") then "D"
  elif test("^TIMESTAMP"; "i") then "T"
  elif test("^CLOB"; "i") then "S"
  elif test("^BLOB"; "i") then "S"
  else "S"
  end;

# Helper to check if column name is in primary key list
def is_pk($pk_cols):
  . as $col_name |
  ([$pk_cols[] | select(. == $col_name)] | length) > 0;

# Helper to check if column has NOT NULL constraint
def is_not_null:
  [(.annotations // [])[] | select(.type == "ColumnIsNotNullable")] | length > 0;

# Extract table name and schema from fully qualified name
def split_table_name:
  if contains(".") then
    split(".") | { schema: .[0], table: .[1] }
  else
    { schema: null, table: . }
  end;

# Title case helper
def title_case:
  gsub("_"; " ") | split(" ") | map(if length > 0 then (.[0:1] | ascii_upcase) + (.[1:] | ascii_downcase) else . end) | join(" ");

# Create a string value expression
def str($v): { type: "StringPrimitive", value: $v };

# Create a number value expression
def num($v): { type: "LongPrimitive", value: $v };

# Create a null expression
def null_val: { type: "NullPrimitive" };

# Create column reference
def col($n): { name: $n, quoted: false };

# Create a column expression for WHERE/SET clauses
def col_expr($n): { type: "Column", column: { name: $n, quoted: false } };

# Create an equality comparison
def eq($l; $r): { type: "Comparison", lhs: $l, op: "Eq", rhs: $r };

# Create an AND expression (uses Arithmetic type)
def and_expr($l; $r): { type: "Arithmetic", lhs: $l, op: "And", rhs: $r };

# Build a 3-way AND for APP_NAME, TBL_NAME, COL_NAME
def where_clause($tbl; $col):
  and_expr(
    and_expr(
      eq(col_expr("APP_NAME"); str("{{APP_NAME}}"));
      eq(col_expr("TBL_NAME"); str($tbl))
    );
    eq(col_expr("COL_NAME"); str($col))
  );

# Check if column should be excluded (system columns like ITS)
def is_system_column:
  . as $name |
  ($name == "ITS" or $name == "ACTION" or $name == "MODIFIED_BY" or $name == "MODIFIED_AT" or
   $name == "ACCEPTED_BY" or $name == "ACCEPTED_AT" or $name == "STATUS" or $name == "STATUS_AT");

# =====================================================
# Extract bindings and statements
# =====================================================

# Get schema settings from bindings (optional YAML)
(.bindings.schema // {}) as $schema_meta |

# Get all successful parse results
[.fileResults[].statementResults[].parseResult | select(.success == true) | .value] |

# Separate CREATE TABLE and ALTER TABLE statements
# Exclude _AT (audit trail) and _HT (historical) tables
{
  creates: [.[] | select(.type == "CreateTable") | select(.table.name | test("_(AT|HT)$") | not)],
  alters: [.[] | select(.type == "AlterTable") | select(.tableName | test("_(AT|HT)$") | not)]
} |

# =====================================================
# Process CREATE TABLE statements
# =====================================================
.creates as $creates |
.alters as $alters |

# Transform CREATE TABLEs to intermediate format
[$creates[] |
  . as $create |
  # Table name is at .table.name, may include schema prefix
  ($create.table.name | split_table_name) as $names |
  # Use explicit schema field if available, otherwise from qualified name
  ($create.table.schema // $names.schema // $schema_meta.TBL_SCHEMA // "GUI_XMDM") as $table_schema |
  ($names.table + "_AT") as $default_at_table |

  # Parse table-level metadata from sourceComment
  (($create.table.sourceComment // []) | parse_source_comments) as $table_meta |

  # Get primary key column names from table.primaryKey
  ([($create.table.primaryKey.columns // [])[]]) as $pk_cols |

  # Columns are directly at .table.columns
  ($create.table.columns // []) as $columns |

  {
    table_name: $names.table,
    schema: $table_schema,
    at_table: ($table_meta.TBL_NAME_AT // $default_at_table),
    gui_name: ($table_meta.TBL_GUI_NAME // ($names.table | title_case)),
    short_name: ($table_meta.TBL_GUI_NAME_SHORT // null),
    display_order: ($table_meta.TBL_DISPLAY_ORDER // null),
    has_ht: ($table_meta.HISTORICAL_TBL_FLAG // null),
    columns: [
      $columns | to_entries[] |
      # Parse column-level metadata from sourceComment
      ((.value.sourceComment // []) | parse_source_comments) as $col_meta |
      {
        name: .value.name,
        gui_name: ($col_meta.COL_GUI_NAME // (.value.name | title_case)),
        display_order: ($col_meta.COL_DISPLAY_ORDER // (.key + 1)),
        is_pk: ($col_meta.COL_PK // (if (.value.name | is_pk($pk_cols)) then "Y" else "N" end)),
        is_required: ($col_meta.COL_REQUIRED // (if (.value.nullable == false) then "Y" else "N" end)),
        datatype: (.value.dataType.type | datatype_code),
        desc: ($col_meta.COL_DESC // null),
        format: ($col_meta.COL_FORMAT // null),
        validator: ($col_meta.COL_VALIDATOR // null),
        multiline: ($col_meta.COL_MULTILINE // null),
        valid_interval: ($col_meta.COL_VALID_INTERVAL_COLUMN // null),
        lookup_sql: ($col_meta.LKP_SQL_STMT1 // null),
        lookup_label: ($col_meta.LKP_SQL_LABEL // null),
        lookup_value: ($col_meta.LKP_SQL_VALUE // null)
      }
    ]
  }
] as $tables |

# =====================================================
# Generate output statements
# =====================================================
[
  # ----- CREATE TABLE: Table inserts -----
  ($tables | to_entries[] |
    . as $entry |
    .value as $t |
    ($entry.key + 1) as $default_order |
    # Use metadata from sourceComment (already in $t)
    ($t.gui_name) as $gui_name |
    ($t.short_name // ($gui_name | .[0:31])) as $short_name |
    ($t.at_table) as $at_table |
    ($t.display_order // $default_order) as $display_order |
    {
      type: "Insert",
      table: col("GUI_XMDM.XMDM_CONF_TABLE"),
      columns: [
        col("APP_NAME"), col("TBL_NAME"), col("TBL_GUI_NAME"), col("TBL_DESC"),
        col("TBL_DISPLAY_ORDER"), col("TBL_GUI_NAME_SHORT"), col("TBL_SCHEMA"),
        col("TBL_NAME_AT"), col("HISTORICAL_TBL_FLAG")
      ],
      values: {
        type: "ExplicitInsert",
        values: [[
          str($schema_meta.APP_NAME // "{{APP_NAME}}"),
          str($t.table_name),
          str($gui_name),
          str(""),
          num($display_order),
          str($short_name),
          str($t.schema),
          str($at_table),
          (if $t.has_ht != null then str($t.has_ht) else null_val end)
        ]]
      },
      orReplace: false
    }
  ),

  # ----- CREATE TABLE: Column inserts -----
  ($tables[] |
    . as $t |
    .columns[] |
    # Filter out system columns (ITS, etc.)
    select(.name | is_system_column | not) |
    . as $col |
    # Metadata is already in $col from sourceComment parsing
    {
      type: "Insert",
      table: col("GUI_XMDM.XMDM_CONF_COLUMN"),
      columns: ([
        col("APP_NAME"), col("TBL_NAME"), col("COL_NAME"), col("COL_GUI_NAME"),
        col("COL_DESC"), col("COL_FORMAT"), col("COL_DISPLAY_ORDER"),
        col("COL_PK"), col("COL_REQUIRED"), col("COL_EDITABLE"), col("COL_DATATYPE"),
        (if $col.validator != null then col("COL_VALIDATOR") else empty end),
        (if $col.multiline != null then col("COL_MULTILINE") else empty end),
        (if $col.valid_interval != null then col("COL_VALID_INTERVAL_COLUMN") else empty end),
        (if $col.lookup_sql != null then col("LKP_SQL_STMT1"), col("LKP_SQL_LABEL"), col("LKP_SQL_VALUE") else empty end)
      ]),
      values: {
        type: "ExplicitInsert",
        values: [([
          str($schema_meta.APP_NAME // "{{APP_NAME}}"),
          str($t.table_name),
          str($col.name),
          str($col.gui_name),
          str($col.desc // $col.name),
          (if $col.format != null then str($col.format) else null_val end),
          num($col.display_order),
          str($col.is_pk),
          str($col.is_required),
          str("Y"),
          str($col.datatype),
          (if $col.validator != null then str($col.validator) else empty end),
          (if $col.multiline != null then str($col.multiline) else empty end),
          (if $col.valid_interval != null then str($col.valid_interval) else empty end),
          (if $col.lookup_sql != null then str($col.lookup_sql), str($col.lookup_label // ""), str($col.lookup_value // "") else empty end)
        ])]
      },
      orReplace: false
    }
  ),

  # ----- ALTER TABLE ADD: Insert new columns -----
  # Note: ALTER TABLE columns don't have sourceComment in current AST, use defaults
  ($alters[] | select(.action.type == "AddColumn") |
    (.tableName | split_table_name) as $names |
    . as $alter |
    .action.column as $col |
    # Filter out system columns
    select($col.name | is_system_column | not) |
    # Parse column metadata from sourceComment if available
    (($col.sourceComment // []) | parse_source_comments) as $col_meta |
    # Use metadata or defaults
    ($col_meta.COL_GUI_NAME // ($col.name | title_case)) as $gui_name |
    ($col_meta.COL_REQUIRED // (if ($col.nullable == false) then "Y" else "N" end)) as $required |
    ($col_meta.COL_DISPLAY_ORDER // 99) as $display_order |
    ($col_meta.COL_DESC // $col.name) as $desc |
    ($col_meta.COL_PK // "N") as $is_pk |
    {
      type: "Insert",
      table: col("GUI_XMDM.XMDM_CONF_COLUMN"),
      columns: ([
        col("APP_NAME"), col("TBL_NAME"), col("COL_NAME"), col("COL_GUI_NAME"),
        col("COL_DESC"), col("COL_FORMAT"), col("COL_DISPLAY_ORDER"),
        col("COL_PK"), col("COL_REQUIRED"), col("COL_EDITABLE"), col("COL_DATATYPE"),
        (if $col_meta.COL_VALIDATOR != null then col("COL_VALIDATOR") else empty end),
        (if $col_meta.COL_MULTILINE != null then col("COL_MULTILINE") else empty end),
        (if $col_meta.COL_VALID_INTERVAL_COLUMN != null then col("COL_VALID_INTERVAL_COLUMN") else empty end),
        (if $col_meta.LKP_SQL_STMT1 != null then col("LKP_SQL_STMT1"), col("LKP_SQL_LABEL"), col("LKP_SQL_VALUE") else empty end)
      ]),
      values: {
        type: "ExplicitInsert",
        values: [([
          str($schema_meta.APP_NAME // "{{APP_NAME}}"),
          str($names.table),
          str($col.name),
          str($gui_name),
          str($desc),
          (if $col_meta.COL_FORMAT != null then str($col_meta.COL_FORMAT) else null_val end),
          num($display_order),
          str($is_pk),
          str($required),
          str("Y"),
          str($col.dataType.type | datatype_code),
          (if $col_meta.COL_VALIDATOR != null then str($col_meta.COL_VALIDATOR) else empty end),
          (if $col_meta.COL_MULTILINE != null then str($col_meta.COL_MULTILINE) else empty end),
          (if $col_meta.COL_VALID_INTERVAL_COLUMN != null then str($col_meta.COL_VALID_INTERVAL_COLUMN) else empty end),
          (if $col_meta.LKP_SQL_STMT1 != null then str($col_meta.LKP_SQL_STMT1), str($col_meta.LKP_SQL_LABEL // ""), str($col_meta.LKP_SQL_VALUE // "") else empty end)
        ])]
      },
      orReplace: false
    }
  ),

  # ----- ALTER TABLE MODIFY: Update column metadata -----
  ($alters[] | select(.action.type == "ModifyColumn") |
    (.tableName | split_table_name) as $names |
    .action.column as $col |
    {
      type: "OracleUpdate",
      table: col("GUI_XMDM.XMDM_CONF_COLUMN"),
      set: [
        { name: col("COL_DATATYPE"), expression: str($col.dataType.type | datatype_code) },
        { name: col("COL_REQUIRED"), expression: str(if ($col.nullable == false) then "Y" else "N" end) }
      ],
      where: where_clause($names.table; $col.name)
    }
  ),

  # ----- ALTER TABLE DROP COLUMN: Delete column metadata -----
  ($alters[] | select(.action.type == "DropColumn") |
    (.tableName | split_table_name) as $names |
    .action.name as $col_name |
    {
      type: "OracleDelete",
      table: col("GUI_XMDM.XMDM_CONF_COLUMN"),
      where: where_clause($names.table; $col_name)
    }
  ),

  # ----- ALTER TABLE RENAME COLUMN: Update column name -----
  ($alters[] | select(.action.type == "RenameColumn") |
    (.tableName | split_table_name) as $names |
    .action as $rename |
    {
      type: "OracleUpdate",
      table: col("GUI_XMDM.XMDM_CONF_COLUMN"),
      set: [
        { name: col("COL_NAME"), expression: str($rename.newName) },
        { name: col("COL_GUI_NAME"), expression: str($rename.newName | title_case) },
        { name: col("COL_DESC"), expression: str($rename.newName) }
      ],
      where: where_clause($names.table; $rename.oldName)
    }
  )
]
