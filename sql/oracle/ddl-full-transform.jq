# DDL Full Transform - Derived Tables + XMDM_CONF Metadata
# Generates _AT (audit trail) and _HT (history) tables + XMDM_CONF MERGE statements
#
# @bind metadata {dir}/{file}.yaml optional
# @write-map metadata {key}-metadata.sql
#
# Usage:
#   sequala plan --source-transform 'jq:sql/oracle/ddl-full-transform.jq' ...
#
# Generates:
#   - statements: DDL for _AT and _HT derived tables (used for schema diffing)
#   - metadata: XMDM_CONF_TABLE and XMDM_CONF_COLUMN MERGE statements (written via @write-map)
#
# Input YAML format (from dump-unified):
#   TABLE_NAME:
#     TBL_DISPLAY_ORDER: 1
#     TBL_GUI_NAME: "Display Name"
#     TBL_GUI_NAME_SHORT: "Short Name"
#     TBL_NAME_AT: "TABLE_NAME_AT"
#     HISTORICAL_TBL_FLAG: "Y"
#     columns:
#       COLUMN_NAME:
#         COL_GUI_NAME: "Column Display Name"
#         COL_DESC: "Description"
#         COL_DISPLAY_ORDER: 1
#         COL_PK: "Y"
#         COL_REQUIRED: "Y"

# =====================================================
# Derived Tables Helpers (from ddl-to-derived-tables.jq)
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

def default_its_column:
  {
    name: "ITS",
    dataType: { type: "Timestamp", withTimeZone: false },
    nullable: true,
    default: "CURRENT_TIMESTAMP",
    options: { invisible: false }
  };

def apply_column_overrides($overrides):
  . as $columns |
  ([$columns[].name]) as $existing_names |
  ([$overrides | to_entries[] | select(.value.skip == true) | .key]) as $skip_names |
  ([
    $columns[] |
    .name as $col_name |
    select($col_name | IN($skip_names[]) | not) |
    ($overrides[$col_name] // null) as $override |
    if $override != null then
      (if $override.nullable != null then .nullable = $override.nullable else . end) |
      (if $override.dataType != null then .dataType = $override.dataType else . end)
    else
      .
    end
  ]) as $updated_columns |
  ([
    $overrides | to_entries[] |
    select(.key | IN($existing_names[]) | not) |
    select(.value.skip != true) |
    .key as $col_name |
    .value as $override |
    if $col_name == "ITS" then
      default_its_column |
      (if $override.nullable != null then .nullable = $override.nullable else . end) |
      (if $override.dataType != null then .dataType = $override.dataType else . end)
    else
      if $override.dataType != null then
        {
          name: $col_name,
          dataType: $override.dataType,
          nullable: ($override.nullable // true),
          options: { invisible: false }
        }
      else
        empty
      end
    end
  ]) as $new_columns |
  $updated_columns + $new_columns;

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
# Metadata MERGE Helpers
# =====================================================

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

def is_pk($pk_cols):
  . as $col_name |
  ([$pk_cols[] | select(. == $col_name)] | length) > 0;

def title_case:
  gsub("_"; " ") | split(" ") | map(if length > 0 then (.[0:1] | ascii_upcase) + (.[1:] | ascii_downcase) else . end) | join(" ");

def str($v): { type: "StringPrimitive", value: $v };
def num($v): { type: "LongPrimitive", value: $v };
def null_val: { type: "NullPrimitive" };
def col($n): { name: $n, quoted: false };

def is_system_column:
  . as $name |
  ($name == "ITS" or $name == "ACTION" or $name == "MODIFIED_BY" or $name == "MODIFIED_AT" or
   $name == "ACCEPTED_BY" or $name == "ACCEPTED_AT" or $name == "STATUS" or $name == "STATUS_AT");

# Build a column reference: { type: "Column", column: {name: col}, table: {name: alias} }
def col_ref($col_name; $table_alias):
  {
    type: "Column",
    column: { name: $col_name, quoted: false },
    table: { name: $table_alias, quoted: false }
  };

# Build comparison: col1 = col2
def eq_comparison($left; $right):
  {
    type: "Comparison",
    lhs: $left,
    op: "EQ",
    rhs: $right
  };

# Build AND of multiple conditions
def and_conditions($conditions):
  if ($conditions | length) == 0 then null
  elif ($conditions | length) == 1 then $conditions[0]
  else
    $conditions | reduce .[1:][] as $cond (.[0]; {type: "Arithmetic", lhs: ., op: "AND", rhs: $cond})
  end;

# Build a MERGE statement
# $target_table: full table name (e.g., "GUI_XMDM.XMDM_CONF_TABLE")
# $pk_cols: array of primary key column names
# $all_cols: array of {name, value} for all columns
def make_merge($target_table; $pk_cols; $all_cols):
  # Build SELECT targets for USING clause
  ([$all_cols[] | {
    type: "SelectTargetExpr",
    expr: .value,
    alias: .name
  }]) as $select_targets |

  # Build ON condition (t.pk1 = s.pk1 AND t.pk2 = s.pk2)
  ([$pk_cols[] | eq_comparison(col_ref(.; "t"); col_ref(.; "s"))]) as $pk_conditions |
  (and_conditions($pk_conditions)) as $on_condition |

  # Build UPDATE SET for non-PK columns
  ([$all_cols[] | select(.name | IN($pk_cols[]) | not) | [col(.name), col_ref(.name; "s")]]) as $update_set |

  # Build INSERT columns and values
  ([$all_cols[] | col(.name)]) as $insert_cols |
  ([$all_cols[] | col_ref(.name; "s")]) as $insert_vals |

  {
    type: "Merge",
    intoTable: col($target_table),
    intoAlias: "t",
    usingSource: {
      type: "MergeSourceSelect",
      query: {
        target: $select_targets,
        from: [{ table: { name: "DUAL", quoted: false } }],
        distinct: false
      },
      alias: "s"
    },
    onCondition: $on_condition,
    whenMatched: (if ($update_set | length) > 0 then { set: $update_set } else null end),
    whenNotMatched: {
      columns: $insert_cols,
      values: $insert_vals
    }
  };

# =====================================================
# Main Processing
# =====================================================

# Get metadata (injected by CLI from YAML via @bind)
(
  if (. | type) == "array" then {}
  elif .bindings then (.bindings.metadata // {})
  else {}
  end
) as $metadata |

# Build set of table names that should have _HT (HISTORICAL_TBL_FLAG: Y)
([$metadata | to_entries[] | select(.value.HISTORICAL_TBL_FLAG == "Y") | .key]) as $ht_tables |

# Get statements array
(
  if (. | type) == "array" then .
  elif .statements then .statements
  else [.fileResults[].statementResults[].parseResult | select(.success == true) | .value]
  end
) as $all_statements |

# Get base tables (excluding _AT and _HT)
[$all_statements[] | select(.type == "CreateTable") | select(.table.name | test("_(AT|HT)$") | not)] as $base_tables |

# Get the schema name from the first table
($base_tables[0].table.schema // "GUI_XMDM") as $schema_name |

# =====================================================
# Generate Derived Tables (_AT and _HT)
# =====================================================
($all_statements + [$base_tables[] |
  .table as $table |
  ($table.name | if test("\\.") then split(".")[1] else . end) as $table_name_only |
  ($metadata[$table_name_only] // {}) as $table_meta |
  ($table_meta.TBL_NAME_AT // ($table.name + "_AT")) as $at_table_name |
  ([$table.columns[] | select(.name | is_audit_column | not)]) as $base_columns |
  ($base_columns | to_entries | map(select(.value.name == "ITS")) | .[0].key // null) as $its_index |
  (if $its_index != null then
    ($base_columns[0:$its_index]) + audit_columns + ($base_columns[$its_index:])
  else
    $base_columns + audit_columns
  end) as $at_columns_base |
  ($at_table_name | if test("\\.") then split(".")[1] else . end) as $at_table_name_only |
  (($metadata[$at_table_name_only] // {}).columns // {}) as $at_overrides |
  ($at_columns_base | apply_column_overrides($at_overrides)) as $at_columns |
  make_derived_table($table; $at_table_name; $at_columns),
  if ($table_name_only | IN($ht_tables[])) then
    (($metadata[$table_name_only + "_HT"] // {}).columns // {}) as $ht_overrides |
    ($base_columns | apply_column_overrides($ht_overrides)) as $ht_columns |
    make_derived_table($table; ($table.name + "_HT"); $ht_columns)
  else
    empty
  end
]) as $derived_statements |

# =====================================================
# Generate XMDM_CONF MERGE Statements
# =====================================================
([
  # Table merges - XMDM_CONF_TABLE PK: (APP_NAME, TBL_NAME)
  ($base_tables | to_entries[] |
    .key as $idx |
    .value.table as $table |
    ($table.name | if test("\\.") then split(".")[1] else . end) as $table_name_only |
    ($metadata[$table_name_only] // {}) as $table_meta |
    ($table_meta.TBL_GUI_NAME // ($table_name_only | title_case)) as $gui_name |
    ($table_meta.TBL_GUI_NAME_SHORT // $gui_name) as $short_name |
    ($table_meta.TBL_NAME_AT // ($table.name + "_AT")) as $at_table |
    ($table_meta.TBL_DISPLAY_ORDER // ($idx + 1)) as $display_order |
    ($table_meta.HISTORICAL_TBL_FLAG // null) as $ht_flag |
    make_merge("GUI_XMDM.XMDM_CONF_TABLE"; ["APP_NAME", "TBL_NAME"]; [
      { name: "APP_NAME", value: str($schema_name) },
      { name: "TBL_NAME", value: str($table_name_only) },
      { name: "TBL_GUI_NAME", value: str($gui_name) },
      { name: "TBL_DESC", value: str("") },
      { name: "TBL_DISPLAY_ORDER", value: num($display_order) },
      { name: "TBL_GUI_NAME_SHORT", value: str($short_name) },
      { name: "TBL_SCHEMA", value: str($schema_name) },
      { name: "TBL_NAME_AT", value: str($at_table) },
      { name: "HISTORICAL_TBL_FLAG", value: (if $ht_flag != null then str($ht_flag) else null_val end) }
    ])
  ),

  # Column merges - XMDM_CONF_COLUMN PK: (APP_NAME, TBL_NAME, COL_NAME)
  ($base_tables[] |
    .table as $table |
    ($table.name | if test("\\.") then split(".")[1] else . end) as $table_name_only |
    ($metadata[$table_name_only] // {}) as $table_meta |
    ($table_meta.columns // {}) as $col_meta_map |
    ($table.columns | to_entries[] |
      .key as $col_idx |
      .value as $col |
      select($col.name | is_system_column | not) |
      ($col_meta_map[$col.name] // {}) as $col_meta |
      ($col_meta.COL_GUI_NAME // ($col.name | title_case)) as $gui_name |
      ($col_meta.COL_DESC // $col.name) as $desc |
      ($col_meta.COL_DISPLAY_ORDER // ($col_idx + 1)) as $display_order |
      ($col_meta.COL_PK // "N") as $is_pk |
      ($col_meta.COL_REQUIRED // (if $col.nullable == false then "Y" else "N" end)) as $required |
      ($col.dataType.type // "Varchar2" | datatype_code) as $datatype |
      # Build the columns array dynamically based on what metadata is present
      ([
        { name: "APP_NAME", value: str($schema_name) },
        { name: "TBL_NAME", value: str($table_name_only) },
        { name: "COL_NAME", value: str($col.name) },
        { name: "COL_GUI_NAME", value: str($gui_name) },
        { name: "COL_DESC", value: str($desc) },
        { name: "COL_FORMAT", value: (if $col_meta.COL_FORMAT != null then str($col_meta.COL_FORMAT) else null_val end) },
        { name: "COL_DISPLAY_ORDER", value: num($display_order) },
        { name: "COL_PK", value: str($is_pk) },
        { name: "COL_REQUIRED", value: str($required) },
        { name: "COL_EDITABLE", value: str("Y") },
        { name: "COL_DATATYPE", value: str($datatype) }
      ] + (if $col_meta.COL_VALIDATOR != null then [{ name: "COL_VALIDATOR", value: str($col_meta.COL_VALIDATOR) }] else [] end)
        + (if $col_meta.COL_MULTILINE != null then [{ name: "COL_MULTILINE", value: str($col_meta.COL_MULTILINE) }] else [] end)
        + (if $col_meta.COL_VALID_INTERVAL_COLUMN != null then [{ name: "COL_VALID_INTERVAL_COLUMN", value: str($col_meta.COL_VALID_INTERVAL_COLUMN) }] else [] end)
        + (if $col_meta.LKP_SQL_STMT1 != null then [
            { name: "LKP_SQL_STMT1", value: str($col_meta.LKP_SQL_STMT1) },
            { name: "LKP_SQL_LABEL", value: str($col_meta.LKP_SQL_LABEL // "") },
            { name: "LKP_SQL_VALUE", value: str($col_meta.LKP_SQL_VALUE // "") }
          ] else [] end)) as $all_cols |
      make_merge("GUI_XMDM.XMDM_CONF_COLUMN"; ["APP_NAME", "TBL_NAME", "COL_NAME"]; $all_cols)
    )
  )
]) as $metadata_merges |

# =====================================================
# Output
# =====================================================
{
  statements: $derived_statements,
  metadata: {
    ($schema_name): $metadata_merges
  }
}
