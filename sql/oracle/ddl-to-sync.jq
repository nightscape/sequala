# DDL to Sync Transform
# Generates _AT/_HT derived tables + INSERT statements for XMDM_CONF tables
#
# @write-map inserts {key}-metadata.sql
#
# Usage:
#   sequala sync --source-transform 'jq:sql/oracle/ddl-to-sync.jq' --write-to sql/oracle/MIGRATIONS/ ...
#
# Reads metadata from embedded comments in DDL:
#   -- @TBL_GUI_NAME: Display Name
#   -- @TBL_DISPLAY_ORDER: 1
#   -- @HISTORICAL_TBL_FLAG: Y
#   -- @COL_GUI_NAME: Column Name
#   -- @COL_PK: Y
#
# Generates:
#   - statements: Original DDL + _AT/_HT derived tables (for schema diffing)
#   - inserts: INSERT statements grouped by schema (written via @write-map)

# =====================================================
# Parse @KEY: value from sourceComment
# =====================================================
def parse_source_comments:
  if . == null or (. | length) == 0 then {}
  else
    [.[] | select(.type == "line") | .text] |
    reduce .[] as $line (
      { result: [], current: null };
      if .current != null then
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
    (if .current != null then .result + [.current] else .result end) |
    [.[] | capture("^@(?<key>[A-Z_0-9]+):\\s*(?<value>.*)$") // empty] |
    [.[] | {key: .key, value: .value}] |
    from_entries
  end;

# =====================================================
# Derived Tables Helpers
# =====================================================
def make_column(name; dataType):
  {
    name: name,
    dataType: dataType,
    nullable: true,
    options: { invisible: false }
  };

def audit_columns:
  [
    make_column("ACTION"; { type: "OracleChar", length: 1, sizeSemantics: "BYTE" }),
    make_column("MODIFIED_BY"; { type: "Varchar2", length: 15, sizeSemantics: "BYTE" }),
    make_column("MODIFIED_AT"; { type: "Timestamp", withTimeZone: false }),
    make_column("ACCEPTED_BY"; { type: "Varchar2", length: 15, sizeSemantics: "BYTE" }),
    make_column("ACCEPTED_AT"; { type: "Timestamp", withTimeZone: false }),
    make_column("STATUS"; { type: "OracleChar", length: 1, sizeSemantics: "BYTE" }),
    make_column("STATUS_AT"; { type: "Timestamp", withTimeZone: false })
  ];

def is_audit_column:
  . as $name |
  ($name == "ACTION" or $name == "MODIFIED_BY" or $name == "MODIFIED_AT" or
   $name == "ACCEPTED_BY" or $name == "ACCEPTED_AT" or $name == "STATUS" or $name == "STATUS_AT");

def is_system_column:
  . as $name |
  ($name == "ITS" or ($name | is_audit_column));

def make_derived_table($table; $name; $columns):
  {
    type: "CreateTable",
    table: {
      name: $name,
      schema: $table.schema,
      columns: $columns,
      indexes: [],
      foreignKeys: [],
      checks: [],
      uniques: [],
      options: $table.options
    },
    orReplace: false,
    ifNotExists: false
  };

# =====================================================
# INSERT Statement Helpers
# =====================================================
def str($v): { type: "StringPrimitive", value: $v };
def num($v): { type: "LongPrimitive", value: $v };
def null_val: { type: "NullPrimitive" };
def col($n): { name: $n, quoted: false };

def datatype_code:
  if test("^Varchar"; "i") then "S"
  elif test("^Char"; "i") then "S"
  elif test("^NVarchar"; "i") then "S"
  elif test("^OracleChar"; "i") then "S"
  elif test("^Number"; "i") then "N"
  elif test("^Float"; "i") then "N"
  elif test("^Integer"; "i") then "N"
  elif test("^Date"; "i") then "D"
  elif test("^Timestamp"; "i") then "T"
  elif test("^Clob"; "i") then "S"
  elif test("^Blob"; "i") then "S"
  else "S"
  end;

def title_case:
  gsub("_"; " ") | split(" ") | map(if length > 0 then (.[0:1] | ascii_upcase) + (.[1:] | ascii_downcase) else . end) | join(" ");

# Build an INSERT statement
def make_insert($target_table; $columns; $values):
  {
    type: "Insert",
    table: col($target_table),
    columns: [$columns[] | col(.)],
    values: {
      type: "ExplicitInsert",
      values: [$values]
    },
    orReplace: false
  };

# =====================================================
# Main Processing
# =====================================================

# Get statements from various input formats
(
  if (. | type) == "array" then .
  elif .statements then .statements
  else [.fileResults[].statementResults[].parseResult | select(.success == true) | .value]
  end
) as $all_statements |

# Get base tables (excluding _AT and _HT)
[$all_statements[] | select(.type == "CreateTable") | select(.table.name | test("_(AT|HT)$") | not)] as $base_tables |

# =====================================================
# Generate Derived Tables (_AT and _HT)
# =====================================================
([$base_tables[] |
  .table as $table |
  ($table.name | if test("\\.") then split(".")[1] else . end) as $table_name_only |
  (($table.sourceComment // []) | parse_source_comments) as $table_meta |
  ($table_meta.TBL_NAME_AT // ($table.name + "_AT")) as $at_table_name |
  ([$table.columns[] | select(.name | is_audit_column | not) | del(.comment) | del(.sourceComment)]) as $base_columns |
  ($base_columns | to_entries | map(select(.value.name == "ITS")) | .[0].key // null) as $its_index |
  (if $its_index != null then
    ($base_columns[0:$its_index]) + audit_columns + ($base_columns[$its_index:])
  else
    $base_columns + audit_columns
  end) as $at_columns |
  make_derived_table($table; $at_table_name; $at_columns),
  if $table_meta.HISTORICAL_TBL_FLAG == "Y" then
    make_derived_table($table; ($table.name + "_HT"); $base_columns)
  else
    empty
  end
]) as $derived_tables |

# =====================================================
# Generate XMDM_CONF INSERT Statements (grouped by schema)
# =====================================================

# XMDM_CONF_TABLE columns
["APP_NAME", "TBL_NAME", "TBL_GUI_NAME", "TBL_DESC", "TBL_DISPLAY_ORDER",
 "TBL_GUI_NAME_SHORT", "TBL_SCHEMA", "TBL_NAME_AT", "HISTORICAL_TBL_FLAG"] as $table_cols |

# XMDM_CONF_COLUMN columns
["APP_NAME", "TBL_NAME", "COL_NAME", "COL_GUI_NAME", "COL_DESC", "COL_FORMAT",
 "COL_DISPLAY_ORDER", "COL_PK", "COL_REQUIRED", "COL_EDITABLE", "COL_DATATYPE",
 "COL_VALIDATOR", "COL_MULTILINE", "COL_VALID_INTERVAL_COLUMN", "COL_AUTOINCREMENT", "COL_DEFAULT_VAL"] as $column_cols |

# Generate all INSERT statements with schema info
([$base_tables | to_entries[] |
  .key as $idx |
  .value.table as $table |
  ($table.schema // "GUI_XMDM") as $schema_name |
  ($table.name | if test("\\.") then split(".")[1] else . end) as $table_name_only |
  (($table.sourceComment // []) | parse_source_comments) as $table_meta |
  ($table_meta.TBL_GUI_NAME // ($table_name_only | title_case)) as $gui_name |
  ($table_meta.TBL_GUI_NAME_SHORT // $gui_name) as $short_name |
  ($table_meta.TBL_NAME_AT // ($table.name + "_AT")) as $at_table |
  ($table_meta.TBL_DISPLAY_ORDER // (($idx + 1) | tostring) | tostring | tonumber) as $display_order |
  ($table_meta.HISTORICAL_TBL_FLAG // null) as $ht_flag |

  # Get primary key columns from table constraint
  ([$table.primaryKey.columns[]? // empty] // []) as $pk_cols |

  # Table INSERT
  {
    schema: $schema_name,
    insert: make_insert("GUI_XMDM.XMDM_CONF_TABLE"; $table_cols; [
      str($schema_name),
      str($table_name_only),
      str($gui_name),
      (if $table_meta.TBL_DESC != null and $table_meta.TBL_DESC != "" then str($table_meta.TBL_DESC) else null_val end),
      num($display_order),
      str($short_name),
      str($schema_name),
      str($at_table),
      (if $ht_flag != null then str($ht_flag) else null_val end)
    ])
  },

  # Column INSERTs
  ($table.columns | to_entries[] |
    .key as $col_idx |
    .value as $col |
    select($col.name | is_system_column | not) |

    (($col.sourceComment // []) | parse_source_comments) as $col_meta |
    ($col_meta.COL_GUI_NAME // ($col.name | title_case)) as $col_gui_name |
    ($col_meta.COL_DESC // null) as $desc |
    ($col_meta.COL_DISPLAY_ORDER // (($col_idx + 1) | tostring) | tostring | tonumber) as $col_display_order |
    (if ($col.name | IN($pk_cols[])) then "Y" elif $col_meta.COL_PK == "Y" then "Y" else "N" end) as $is_pk |
    ($col_meta.COL_REQUIRED // (if $col.nullable == false or $is_pk == "Y" then "Y" else "N" end)) as $required |
    ($col.dataType.type // "Varchar2" | datatype_code) as $datatype |

    {
      schema: $schema_name,
      insert: make_insert("GUI_XMDM.XMDM_CONF_COLUMN"; $column_cols; [
        str($schema_name),
        str($table_name_only),
        str($col.name),
        str($col_gui_name),
        (if $desc != null and $desc != "" then str($desc) else null_val end),
        (if $col_meta.COL_FORMAT != null then str($col_meta.COL_FORMAT) else null_val end),
        num($col_display_order),
        str($is_pk),
        str($required),
        str("Y"),
        str($datatype),
        (if $col_meta.COL_VALIDATOR != null then str($col_meta.COL_VALIDATOR) else null_val end),
        (if $col_meta.COL_MULTILINE != null then str($col_meta.COL_MULTILINE) else null_val end),
        (if $col_meta.COL_VALID_INTERVAL_COLUMN != null then str($col_meta.COL_VALID_INTERVAL_COLUMN) else null_val end),
        str($col_meta.COL_AUTOINCREMENT // "N"),
        (if $col_meta.COL_DEFAULT_VAL != null then str($col_meta.COL_DEFAULT_VAL) else null_val end)
      ])
    }
  )
]) as $all_inserts |

# Group inserts by schema
($all_inserts | group_by(.schema) | map({key: .[0].schema, value: [.[].insert]}) | from_entries) as $inserts_by_schema |

# Extract flat list of INSERT statements for data diffing
($all_inserts | map(.insert)) as $insert_statements |

# =====================================================
# Output: statements for diffing + inserts grouped by schema for @write-map
# =====================================================
{
  statements: ($all_statements + $derived_tables + $insert_statements),
  inserts: $inserts_by_schema
}
