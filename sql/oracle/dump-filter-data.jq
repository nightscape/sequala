# Transform XMDM_CONF_TABLE/COLUMN data to metadata YAML
#
# Converts rows from XMDM_CONF_TABLE and XMDM_CONF_COLUMN tables into
# the same YAML metadata structure as merge-ddl-and-metadata.jq produces.
# Groups output by TBL_SCHEMA column to create one YAML file per schema.
#
# Input JSON structure (from dump --data):
# {
#   "tables": [
#     {
#       "schema": "GUI_XMDM",
#       "table": "XMDM_CONF_TABLE",
#       "data": {
#         "columns": ["TBL_NAME", "TBL_SCHEMA", "TBL_GUI_NAME", ...],
#         "rows": [{"TBL_NAME": "...", "TBL_SCHEMA": "GUI_XMDM_F7", ...}]
#       }
#     }
#   ]
# }
#
# Output: One YAML file per TBL_SCHEMA value (e.g., GUI_XMDM_F7-metadata.yaml)
#
# Usage:
#   sequala dump --data "GUI_XMDM.XMDM_CONF%" --dataFilter sql/oracle/dump-filter-data.jq --outputDir dump/
#
# @write-map metadata {key}-PROD-tables.yaml

# Helper to build column metadata object
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

# Extract table data from XMDM_CONF_TABLE
(.tables | map(select(.table | test("XMDM_CONF_TABLE$"))) | map(.data.rows) | add // []) as $table_rows |

# Extract column data from XMDM_CONF_COLUMN
(.tables | map(select(.table | test("XMDM_CONF_COLUMN$"))) | map(.data.rows) | add // []) as $column_rows |

# Build table info map including schema: { "TABLE_NAME": { schema, gui_name, short_name, at_table, order, has_ht } }
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

# Get unique schemas from table rows
([$table_rows[] | .TBL_SCHEMA // "UNKNOWN"] | unique | map(select(. != null and . != ""))) as $schemas |

# Build metadata grouped by schema
[$schemas[] |
  . as $schema |

  # Get tables for this schema
  ([$table_rows[] | select((.TBL_SCHEMA // "UNKNOWN") == $schema) | .TBL_NAME] | unique) as $schema_tables |

  # Build per-table metadata for this schema
  ([$schema_tables[] |
    . as $tbl_name |
    ($table_meta[$tbl_name] // null) as $tbl_meta |

    # Find all columns for this table
    ([$column_rows[] | select(.TBL_NAME == $tbl_name)] |
      map({key: .COL_NAME, value: build_column_metadata(.)}) |
      from_entries) as $columns_metadata |

    # Build table metadata YAML structure
    ({}
      | if $tbl_meta != null and $tbl_meta.gui_name != null then . + { TBL_GUI_NAME: $tbl_meta.gui_name } else . end
      | if $tbl_meta != null and $tbl_meta.short_name != null then . + { TBL_GUI_NAME_SHORT: $tbl_meta.short_name } else . end
      | if $tbl_meta != null and $tbl_meta.at_table != null then . + { TBL_NAME_AT: $tbl_meta.at_table } else . end
      | if $tbl_meta != null and $tbl_meta.order != null then . + { TBL_DISPLAY_ORDER: $tbl_meta.order } else . end
      | if $tbl_meta != null and $tbl_meta.has_ht == "Y" then . + { HISTORICAL_TBL_FLAG: "Y" } else . end
      | if ($columns_metadata | length) > 0 then . + { columns: $columns_metadata } else . end
    ) as $metadata |

    # Only include if there is actual metadata
    select(($metadata | length) > 0) |
    { key: $tbl_name, value: $metadata }
  ] | from_entries) as $schema_metadata |

  # Only include schemas that have tables with metadata
  select(($schema_metadata | length) > 0) |
  { key: $schema, value: $schema_metadata }
] | from_entries |

# Wrap in metadata key for @write-map
{ metadata: . }
