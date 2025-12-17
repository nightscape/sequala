# Metadata to DDL Transformation
# Converts XMDM_CONF_TABLE and XMDM_CONF_COLUMN INSERT statements to CREATE TABLE with embedded metadata comments
#
# Usage:
#   sequala parse oracle --output 'jq-file-sql:sql/oracle/metadata-to-ddl.jq' --write-to 'schemas/' <configure-file.sql>
#
# @write-map ddl {key}.sql
#
# This is the reverse transformation of ddl-to-metadata.jq
# Input: INSERT statements into GUI_XMDM.XMDM_CONF_TABLE and GUI_XMDM.XMDM_CONF_COLUMN
# Output: CREATE TABLE statements with metadata embedded as -- @KEY: value comments in sourceComment
#
# =============================================================================
# Output Structure
# =============================================================================
#
# For each table configured in XMDM_CONF_TABLE:
#   {key}.sql - CREATE TABLE statement with embedded metadata comments
#
# Example output:
#   -- @TBL_GUI_NAME: Price Monitor
#   -- @TBL_DISPLAY_ORDER: 1
#   CREATE TABLE SCHEMA.TABLE_NAME (
#     -- @COL_GUI_NAME: Product ID
#     -- @COL_PK: Y
#     PRODUCT_ID NUMBER NOT NULL,
#     ...
#   )
#
# Notes:
#   - Data types are inferred from COL_DATATYPE: S->VARCHAR2(256 BYTE), N->NUMBER, D->DATE, T->TIMESTAMP
#   - Primary key constraint is generated from columns where COL_PK='Y'
#   - NOT NULL is added for columns where COL_REQUIRED='Y'
#   - ITS column with DEFAULT CURRENT_TIMESTAMP is automatically added

# Helper to convert COL_DATATYPE code to Oracle type
def datatype_to_oracle:
  if . == "S" then "VARCHAR2(256 BYTE)"
  elif . == "N" then "NUMBER"
  elif . == "D" then "DATE"
  elif . == "T" then "TIMESTAMP"
  else "VARCHAR2(256 BYTE)"
  end;

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

# Helper to get string value
def str_val($columns; $values; $col_name):
  get_value($columns; $values; $col_name) |
  if . != null and .type == "StringPrimitive" then .value else null end;

# Helper to get number value
def num_val($columns; $values; $col_name):
  get_value($columns; $values; $col_name) |
  if . != null and .type == "LongPrimitive" then .value
  elif . != null and .type == "StringPrimitive" then (.value | tonumber? // null)
  else null end;

# Build column metadata object for YAML output (using direct column names)
def build_column_metadata($col):
  {}
  | if $col.gui_name != null and $col.gui_name != "" then . + { COL_GUI_NAME: $col.gui_name } else . end
  | if $col.desc != null and $col.desc != "" and $col.desc != $col.name then . + { COL_DESC: $col.desc } else . end
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

# =====================================================
# Extract all INSERT statements
# =====================================================

[.fileResults[].statementResults[].parseResult | select(.success == true) | .value] |

# Filter to INSERT statements only
[.[] | select(.type == "OracleInsert" or .type == "Insert")] |

# Separate table and column inserts
{
  table_inserts: [.[] | select(.table.name | test("XMDM_CONF_TABLE$"))],
  column_inserts: [.[] | select(.table.name | test("XMDM_CONF_COLUMN$"))]
} |

# =====================================================
# Process table inserts to build table metadata
# =====================================================

.table_inserts as $table_inserts |
.column_inserts as $column_inserts |

# Build table metadata map: { "TABLE_NAME": { schema, gui_name, short_name, at_table, order, app_name } }
([$table_inserts[] |
  .columns as $cols |
  .values.values[0] as $vals |
  (str_val($cols; $vals; "TBL_NAME")) as $tbl_name |
  select($tbl_name != null) |
  {
    key: $tbl_name,
    value: {
      schema: (str_val($cols; $vals; "TBL_SCHEMA") // "GUI_XMDM"),
      gui_name: str_val($cols; $vals; "TBL_GUI_NAME"),
      short_name: str_val($cols; $vals; "TBL_GUI_NAME_SHORT"),
      at_table: str_val($cols; $vals; "TBL_NAME_AT"),
      order: num_val($cols; $vals; "TBL_DISPLAY_ORDER"),
      app_name: str_val($cols; $vals; "APP_NAME")
    }
  }
] | from_entries) as $table_meta |

# Build column metadata grouped by table: { "TABLE_NAME": [ {col_data}, ... ] }
([$column_inserts[] |
  .columns as $cols |
  .values.values[0] as $vals |
  (str_val($cols; $vals; "TBL_NAME")) as $tbl_name |
  select($tbl_name != null) |
  {
    table: $tbl_name,
    col: {
      name: str_val($cols; $vals; "COL_NAME"),
      gui_name: str_val($cols; $vals; "COL_GUI_NAME"),
      desc: str_val($cols; $vals; "COL_DESC"),
      order: num_val($cols; $vals; "COL_DISPLAY_ORDER"),
      pk: str_val($cols; $vals; "COL_PK"),
      required: str_val($cols; $vals; "COL_REQUIRED"),
      datatype: str_val($cols; $vals; "COL_DATATYPE"),
      format: str_val($cols; $vals; "COL_FORMAT"),
      validator: str_val($cols; $vals; "COL_VALIDATOR"),
      multiline: str_val($cols; $vals; "COL_MULTILINE"),
      valid_interval: str_val($cols; $vals; "COL_VALID_INTERVAL_COLUMN"),
      lookup_sql: str_val($cols; $vals; "LKP_SQL_STMT1"),
      lookup_label: str_val($cols; $vals; "LKP_SQL_LABEL"),
      lookup_value: str_val($cols; $vals; "LKP_SQL_VALUE")
    }
  }
] | group_by(.table) | map({ key: .[0].table, value: [.[].col] }) | from_entries) as $columns_by_table |

# =====================================================
# Generate output map: { ddl: { TABLE: [...] } }
# Metadata is embedded in sourceComment on table and columns
# =====================================================

# Build per-table data
([$table_meta | to_entries[] |
  .key as $tbl_name |
  .value as $tbl_meta |
  ($columns_by_table[$tbl_name] // []) as $columns |

  # Sort columns by display order
  ($columns | sort_by(.order // 999)) as $sorted_cols |

  # Get primary key columns
  ([$sorted_cols[] | select(.pk == "Y") | .name]) as $pk_cols |

  # Build column definitions for CREATE TABLE with embedded metadata in sourceComment
  ([$sorted_cols[] |
    # Build column metadata for sourceComment
    (build_column_metadata(.) | build_source_comments) as $col_source_comment |
    {
      name: .name,
      dataType: { type: (.datatype | datatype_to_oracle) },
      nullable: (if .required == "Y" or .pk == "Y" then false else true end),
      sourceComment: $col_source_comment
    }
  ]) as $col_defs |

  # Add ITS column (no metadata comment)
  ($col_defs + [{
    name: "ITS",
    dataType: { type: "TIMESTAMP" },
    nullable: true,
    default: "CURRENT_TIMESTAMP"
  }]) as $all_cols |

  # Build table-level metadata for sourceComment
  ({}
    | if $tbl_meta.gui_name != null then . + { TBL_GUI_NAME: $tbl_meta.gui_name } else . end
    | if $tbl_meta.short_name != null then . + { TBL_GUI_NAME_SHORT: $tbl_meta.short_name } else . end
    | if $tbl_meta.at_table != null then . + { TBL_NAME_AT: $tbl_meta.at_table } else . end
    | if $tbl_meta.order != null then . + { TBL_DISPLAY_ORDER: $tbl_meta.order } else . end
  ) as $table_metadata |

  # Build DDL statement (CreateTable with embedded sourceComment)
  {
    type: "CreateTable",
    orReplace: false,
    ifNotExists: false,
    table: {
      name: ($tbl_meta.schema + "." + $tbl_name),
      columns: $all_cols,
      primaryKey: (if ($pk_cols | length) > 0 then { columns: $pk_cols } else null end),
      sourceComment: ($table_metadata | build_source_comments)
    }
  } as $create_stmt |

  # Output entry for this table
  {
    key: $tbl_name,
    value: [$create_stmt]
  }
] | from_entries) as $tables |

# Restructure for @write-map: { ddl: { TABLE: [...] } }
{
  ddl: $tables
}
