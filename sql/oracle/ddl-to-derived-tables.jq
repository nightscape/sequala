# DDL to Derived Tables Transformation
# Generates _AT (audit trail) and _HT (history) tables from base table DDL
#
# Usage:
#   sequala parse oracle --output 'jq-file-sql:sql/oracle/ddl-to-derived-tables.jq' <ddl-file.sql>
#
# Generates:
#   - _AT tables: base columns + 7 audit columns (ACTION, MODIFIED_BY, etc.) + ITS
#   - _HT tables: only for tables with HISTORICAL_TBL_FLAG: Y in sourceComment
#
# Metadata is read from -- @KEY: value comments in the base table's sourceComment:
#   -- @TBL_NAME_AT: CUSTOM_AT_NAME    (custom _AT table name, default: base_name + "_AT")
#   -- @HISTORICAL_TBL_FLAG: Y         (generate _HT table for this base table)
#
# Notes:
#   - Tables already ending with _AT or _HT are excluded
#   - Primary key constraints are NOT copied
#   - NOT NULL constraints are preserved on base columns
#   - _HT tables are ONLY generated for tables with HISTORICAL_TBL_FLAG: Y in sourceComment

# Parse @KEY: value from sourceComment array (with continuation support)
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

# Get statements from various input formats:
# - Direct array: [stmt, stmt, ...]
# - Object with statements: { "statements": [...], "bindings": {...} }
# - Parse output: { "fileResults": [...] }
(
  if (. | type) == "array" then .
  elif .statements then .statements
  else [.fileResults[].statementResults[].parseResult | select(.success == true) | .value]
  end
) |
# Store all original statements to pass through
. as $all_statements |
# Get base tables (excluding _AT and _HT) for generating derived tables
[.[] | select(.type == "CreateTable") | select(.table.name | test("_(AT|HT)$") | not)] |

# Generate _AT for all tables, _HT only for tables with HISTORICAL_TBL_FLAG: Y in sourceComment
# Output includes: original statements + generated derived tables
$all_statements + [.[] |
  .table as $table |
  ($table.name | if test("\\.") then split(".")[1] else . end) as $table_name_only |

  # Parse table-specific metadata from sourceComment
  (($table.sourceComment // []) | parse_source_comments) as $table_meta |

  # Use TBL_NAME_AT from sourceComment if specified, otherwise default to base + suffix
  ($table_meta.TBL_NAME_AT // ($table.name + "_AT")) as $at_table_name |

  # Base columns without audit columns (strip comments - they belong to base tables only)
  ([$table.columns[] | select(.name | is_audit_column | not) | del(.comment)]) as $base_columns |

  # Find ITS position to insert audit columns before it
  ($base_columns | to_entries | map(select(.value.name == "ITS")) | .[0].key // null) as $its_index |

  # _AT columns: base + audit columns (inserted before ITS if present)
  (if $its_index != null then
    ($base_columns[0:$its_index]) + audit_columns + ($base_columns[$its_index:])
  else
    $base_columns + audit_columns
  end) as $at_columns |

  # Always output _AT table
  make_derived_table($table; $at_table_name; $at_columns),

  # Only output _HT table if HISTORICAL_TBL_FLAG is Y in sourceComment
  if $table_meta.HISTORICAL_TBL_FLAG == "Y" then
    make_derived_table($table; ($table.name + "_HT"); $base_columns)
  else
    empty
  end
]
