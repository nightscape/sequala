# Embed YAML Metadata into DDL as sourceComment
#
# This jq script takes parsed DDL (CREATE TABLE statements) and embeds
# metadata from a YAML file as sourceComment entries (-- @KEY: value format).
#
# The @bind directive below tells the CLI to load the .yaml file with the same
# base name as the input .sql file. For example:
#   Input: sql/oracle/DESIRED_STATE/GUI_XMDM_F7-PROD-tables.sql
#   Binds: sql/oracle/DESIRED_STATE/GUI_XMDM_F7-PROD-tables.yaml -> $metadata
#
# Usage:
#   sequala parse oracle \
#     --output 'jq-file-sql:sql/oracle/embed-yaml-metadata.jq' \
#     --pretty \
#     --write-to 'sql/oracle/OUTPUT/' \
#     'sql/oracle/DESIRED_STATE/GUI_XMDM_F7-PROD-tables.sql'
#
# @bind metadata {dir}/{file}.yaml optional
#
# Output: DDL statements with sourceComment containing metadata

# Helper to build sourceComment array from metadata map
# Input: { "KEY1": "value1", "KEY2": value2, ... }
# Output: [ { "type": "line", "text": "@KEY1: value1" }, ... ]
def build_source_comments:
  [to_entries[] | select(.value != null and .value != "" and .key != "columns") |
   { type: "line", text: ("@" + .key + ": " + (.value | tostring)) }
  ];

# Helper to build column metadata comments
def build_column_comments:
  [to_entries[] | select(.value != null and .value != "") |
   { type: "line", text: ("@" + .key + ": " + (.value | tostring)) }
  ];

# Extract just the table name (without schema prefix)
def table_name_only:
  if . == null then null
  elif type == "object" and has("name") then .name | table_name_only
  elif type == "string" then
    if contains(".") then split(".") | .[-1] else . end
  else tostring end;

# Get metadata from bindings (injected by CLI based on @bind directive)
(.bindings.metadata // {}) as $metadata |

# Process statements and output flat array
[.fileResults[].statementResults[] |
  select(.parseResult.success == true) |
  .parseResult.value |

  # If it's a CreateTable, potentially add metadata
  if .type == "CreateTable" then
    . as $ct |
    ($ct.table.name | table_name_only) as $tbl_name |
    ($metadata[$tbl_name] // null) as $tbl_meta |

    if $tbl_meta != null then
      # Build table-level sourceComment
      ($tbl_meta | build_source_comments) as $table_comments |

      # Update columns with their metadata
      ($ct.table.columns | map(
        . as $col |
        ($tbl_meta.columns[$col.name] // null) as $col_meta |
        if $col_meta != null then
          ($col_meta | build_column_comments) as $col_comments |
          if ($col_comments | length) > 0 then
            . + { sourceComment: $col_comments }
          else . end
        else . end
      )) as $updated_columns |

      # Return updated CreateTable
      .table |= (
        . +
        (if ($table_comments | length) > 0 then { sourceComment: $table_comments } else {} end) +
        { columns: $updated_columns }
      )
    else . end
  else . end
]
