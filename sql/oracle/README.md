# Oracle Schema Migration Pipeline

A system for extracting Oracle database schemas with metadata, storing them as canonical DDL with embedded comments, and generating migrations to restore or sync databases.

## Overview

This pipeline manages Oracle database schemas that use a specific pattern:
- **Base tables** - business data tables
- **Audit tables** (`_AT` suffix) - track changes with audit columns (ACTION, MODIFIED_BY, MODIFIED_AT, etc.)
- **History tables** (`_HT` suffix) - optional historical snapshots (only for tables with `HISTORICAL_TBL_FLAG=Y`)
- **Metadata tables** - `XMDM_CONF_TABLE` and `XMDM_CONF_COLUMN` store GUI configuration

### Goals

1. **Extract** schema and metadata from production Oracle databases
2. **Store** as version-controlled DDL files with embedded metadata comments
3. **Generate** migrations to restore or sync databases from this "desired state"
4. **Ensure** round-trip fidelity (dump → restore should be lossless)

## Quick Start

### Prerequisites

```bash
# Ensure Docker is running
docker --version

# Build sequala JAR (or use sbt/scala-cli)
sbt cli/assembly
```

### Basic Workflow

```bash
cd sql/oracle

# 1. Start Oracle test containers
./oracle-comparison.sh start

# 2. Dump schema from a database
./oracle-comparison.sh dump-unified \
  "system/password@//host:1521/SERVICE" \
  "GUI_XMDM%" \
  "GUI_XMDM.XMDM_CONF%"

# 3. Compare desired state against database
./oracle-comparison.sh compare-desired-state migrations/ \
  "system/password@//host:1521/SERVICE" \
  "GUI_XMDM%"

# 4. Apply migrations (if needed)
# Review migrations/*.sql files first!
./oracle-comparison.sh migrate migrations/GUI_XMDM_F7-migration.sql

# 5. Clean up
./oracle-comparison.sh stop
```

## Commands

### Container Management

```bash
# Start oracle-orig and oracle-new containers with database links
./oracle-comparison.sh start

# Check container status
./oracle-comparison.sh status

# Stop and remove containers
./oracle-comparison.sh stop
```

### Dumping Schema from Database

```bash
# Dump with unified filter (recommended)
./oracle-comparison.sh dump-unified \
  "user/pass@//host:port/SERVICE" \
  "SCHEMA_PATTERN" \
  "DATA_PATTERN"

# Example: dump all GUI_XMDM schemas
./oracle-comparison.sh dump-unified \
  "system/orakel@//localhost:1521/FREEPDB1" \
  "GUI_XMDM%" \
  "GUI_XMDM.XMDM_CONF%"
```

Output: `DESIRED_STATE/{SCHEMA}-PROD-tables.sql` with embedded metadata comments

### Generating Desired State from SQL Files

```bash
# Generate from existing SQL files (alternative to dump)
./oracle-comparison.sh generate-prod-tables [--force]
```

Processes SQL files in schema subdirectories, merges with XMDM_CONF metadata, outputs to `DESIRED_STATE/`.

### Converting Metadata

```bash
# Convert XMDM_CONF INSERTs to DDL with embedded comments
./oracle-comparison.sh metadata-to-ddl \
  PMDS/02_CONFIGURE_XMDM_CONF_TABLES.sql \
  output.sql
```

### Comparing and Generating Migrations

```bash
# Generate migrations (DDL only)
./oracle-comparison.sh compare-desired-state \
  output_dir/ \
  "user/pass@//host:port/SERVICE" \
  "SCHEMA_PATTERN"

# Generate migrations (DDL + data)
./oracle-comparison.sh sync \
  output_dir/ \
  "user/pass@//host:port/SERVICE" \
  "SCHEMA_PATTERN"
```

Output: One migration SQL file per schema

### Applying Migrations

```bash
# Apply migration file to container
./oracle-comparison.sh migrate \
  migrations/GUI_XMDM_F7-migration.sql \
  oracle-new \
  "system/pass@//localhost:1522/FREEPDB1"
```

## JQ Scripts

### Active Scripts

| Script | Purpose | Used By |
|--------|---------|---------|
| `ddl-to-derived-tables.jq` | Generate _AT (always) and _HT (if flagged) from base tables | `compare-desired-state`, `sync` |
| `ddl-to-sync.jq` | Generate _AT/_HT + XMDM_CONF INSERTs for unified sync | `sync` |
| `ddl-to-metadata.jq` | Convert DDL to XMDM_CONF INSERT statements | Legacy |
| `merge-ddl-and-metadata.jq` | Merge SQL files + XMDM_CONF → DDL with embedded comments | `generate-prod-tables` |
| `metadata-to-ddl.jq` | Reverse: XMDM_CONF INSERTs → CREATE TABLE with comments | `metadata-to-ddl` |
| `unified-dump-filter.jq` | Combined DDL+metadata transform with overrides | `dump-unified` |

### Script Details

**ddl-to-derived-tables.jq**
- Reads base table DDL with embedded `@` metadata comments
- Generates `{TABLE}_AT` with audit columns (ACTION, MODIFIED_BY, etc.)
- Generates `{TABLE}_HT` only if `@HISTORICAL_TBL_FLAG: Y` in comments
- Respects custom `@TBL_NAME_AT` for non-standard AT table names

**unified-dump-filter.jq**
- Combines DDL and metadata from database dump
- Embeds XMDM_CONF metadata as `@KEY: value` comments in DDL
- Auto-detects _AT/_HT column overrides (ITS column, nullable differences)
- Outputs single SQL file per schema with all metadata embedded

**ddl-to-sync.jq**
- Generates derived _AT/_HT tables for schema diffing
- Extracts metadata from embedded comments
- Generates XMDM_CONF INSERT statements grouped by schema
- Used by `sync` command for unified DDL + data migrations

## Embedded Metadata Format

DDL files use special comments to preserve metadata from XMDM_CONF tables:

```sql
-- @TBL_GUI_NAME: Price Trade Monitor
-- @TBL_GUI_NAME_SHORT: PRICE_MONITOR
-- @TBL_NAME_AT: PRICE_TRADE_MONITOR_AT
-- @TBL_DISPLAY_ORDER: 4
-- @HISTORICAL_TBL_FLAG: Y
CREATE TABLE GUI_XMDM_F7.PRICE_TRADE_MONITOR (
  -- @COL_GUI_NAME: Product ID
  -- @COL_PK: Y
  -- @COL_DISPLAY_ORDER: 1
  PRODUCT_ID NUMBER NOT NULL,

  -- @COL_GUI_NAME: Span Value
  -- @COL_DISPLAY_ORDER: 2
  -- @COL_VALIDATOR: RANGE(0,1000000)
  SPAN NUMBER(19) NOT NULL,

  ITS TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Table-Level Metadata Keys

- `TBL_GUI_NAME` - Display name in GUI
- `TBL_GUI_NAME_SHORT` - Short display name (max 31 chars)
- `TBL_NAME_AT` - Custom audit table name (default: `{TABLE}_AT`)
- `TBL_DISPLAY_ORDER` - Sort order in GUI
- `HISTORICAL_TBL_FLAG` - `Y` to generate _HT table

### Column-Level Metadata Keys

- `COL_GUI_NAME` - Display name in GUI
- `COL_DESC` - Description
- `COL_DISPLAY_ORDER` - Sort order in GUI
- `COL_PK` - `Y` for primary key columns
- `COL_REQUIRED` - `Y` for NOT NULL
- `COL_EDITABLE` - `Y` if user can edit
- `COL_DATATYPE` - `S`tring, `N`umber, `D`ate, `T`imestamp
- `COL_FORMAT` - Display format pattern
- `COL_VALIDATOR` - Validation rule
- `COL_MULTILINE` - `Y` for multi-line text
- `COL_VALID_INTERVAL_COLUMN` - Column name for validity range
- `COL_AUTOINCREMENT` - `Y` for auto-increment
- `COL_DEFAULT_VAL` - Default value
- `LKP_SQL_STMT1` - Lookup SQL query
- `LKP_SQL_LABEL` - Lookup display column
- `LKP_SQL_VALUE` - Lookup value column

## Derived Table Rules

### Audit Tables (`_AT`)

Generated for **every** base table with these columns added:
- Base table columns (excluding existing audit columns and ITS)
- 7 audit columns inserted before ITS (or at end):
  - `ACTION CHAR(1)` - I/U/D
  - `MODIFIED_BY VARCHAR2(15)` - User who made change
  - `MODIFIED_AT TIMESTAMP` - When changed
  - `ACCEPTED_BY VARCHAR2(15)` - Approver
  - `ACCEPTED_AT TIMESTAMP` - When approved
  - `STATUS CHAR(1)` - Status code
  - `STATUS_AT TIMESTAMP` - Status change time
- `ITS` column (if present in base table)

### History Tables (`_HT`)

Generated **only** when `@HISTORICAL_TBL_FLAG: Y` in base table metadata:
- Contains base table columns (excluding audit columns)
- No additional columns added
- Used for historical snapshots

## Environment Variables

```bash
# Default connection string
export DEFAULT_CONN_STRING="system/password@//localhost:1521/FREEPDB1"

# Default container for docker exec
export DEFAULT_CONTAINER="oracle-orig"

# Oracle passwords
export ORACLE_PASSWORD="orakel"
export APP_USER="xmdm_user"
export APP_USER_PASSWORD="xmdm_pass"

# Sequala execution method
export SEQUALA_JAR="path/to/sequala-cli.jar"
export SEQUALA_MEMORY="-Xmx8g -Xms4g"

# Or use sbt mode
export SEQUALA_SBT_MODE="true"
export SEQUALA_CMD="sbt"
```

## Common Workflows

### 1. Create Desired State from Production

```bash
# Dump all GUI_XMDM schemas from production
./oracle-comparison.sh dump-unified \
  "system/prod_pass@//prod-host:1521/PROD" \
  "GUI_XMDM%" \
  "GUI_XMDM.XMDM_CONF%"

# Output: DESIRED_STATE/{schema}-PROD-tables.sql
# Commit these files to version control
git add DESIRED_STATE/
git commit -m "Update desired state from production"
```

### 2. Deploy Desired State to Staging

```bash
# Compare desired state against staging database
./oracle-comparison.sh sync \
  migrations/ \
  "system/staging_pass@//staging-host:1521/STAGING" \
  "GUI_XMDM%"

# Review migrations/*.sql files

# Apply each migration
for f in migrations/*.sql; do
  ./oracle-comparison.sh migrate "$f" \
    staging-container \
    "system/staging_pass@//staging-host:1521/STAGING"
done
```

### 3. Round-Trip Test (Verify Fidelity)

```bash
# Start with empty database
./oracle-comparison.sh start

# Apply desired state
./oracle-comparison.sh migrate-new

# Re-dump and compare
./oracle-comparison.sh dump-unified \
  "system/orakel@//localhost:1522/FREEPDB1" \
  "GUI_XMDM%" \
  "GUI_XMDM.XMDM_CONF%"

# Should produce zero or minimal differences
diff -r DESIRED_STATE/ DESIRED_STATE_ROUNDTRIP/
```

## Known Issues and Limitations

See `TODO.md` for detailed issue tracking. Summary:

### Fixed Issues
- ✅ INTEGER → NUMBER(10) type normalization
- ✅ VARCHAR2 CHAR_LENGTH vs DATA_LENGTH
- ✅ NUMBER(p,0) ≡ NUMBER(p) equivalence
- ✅ Custom _AT table names from metadata
- ✅ Primary key columns implicitly NOT NULL

### Active Issues
- ⚠️ ITS column type inconsistency (TIMESTAMP vs DATE)
- ⚠️ _HT table existence tracking needs improvement
- ⚠️ Tables without XMDM_CONF metadata excluded from output
- ⚠️ Schema prefixes missing in some source files
- ⚠️ COMMENT statements not exported in dump

### Round-Trip Status
18/32 schemas pass perfect round-trip tests (56% success rate as of 2025-12-16)

## Architecture

```
Production Oracle DB
        ↓
    [dump-unified]
        ↓
DESIRED_STATE/{schema}-PROD-tables.sql
(DDL with embedded @metadata comments)
        ↓
    [sync/compare]
    (via ddl-to-sync.jq)
        ↓
- Original DDL
- Generated _AT tables
- Generated _HT tables (if flagged)
- XMDM_CONF INSERT statements
        ↓
    [SchemaDiffer]
        ↓
migrations/{schema}-migration.sql
        ↓
    [migrate]
        ↓
Target Oracle DB
```

## Files

| File | Purpose |
|------|---------|
| `oracle-comparison.sh` | Main orchestration script |
| `TODO.md` | Issue tracking and progress log |
| `*.jq` | JQ transformation scripts |
| `DESIRED_STATE/` | Canonical DDL files with metadata |
| `MIGRATIONS/` | Generated migration SQL |

## Troubleshooting

### "JAR not found" Error

```bash
# Build the JAR
cd /path/to/sequala
sbt cli/assembly

# Or use scala-cli mode
export SEQUALA_SBT_MODE="true"
```

### Slow Performance

```bash
# Use pre-built JAR instead of sbt
export SEQUALA_SBT_MODE="false"
export SEQUALA_JAR="cli/target/scala-3.3.6/sequala-cli-*.jar"

# Increase memory
export SEQUALA_MEMORY="-Xmx16g -Xms8g"
```

### Container Connection Issues

```bash
# Check containers are running
./oracle-comparison.sh status

# Restart containers
./oracle-comparison.sh stop
./oracle-comparison.sh start

# Check logs
docker logs oracle-orig
docker logs oracle-new
```

### Migration Failures

```bash
# Check for schema existence
echo "SELECT username FROM all_users WHERE username LIKE 'GUI_XMDM%';" | \
  docker exec -i oracle-orig sqlplus -s system/orakel@//localhost/FREEPDB1

# Create missing schemas
docker exec -i oracle-orig sqlplus -s system/orakel@//localhost/FREEPDB1 <<EOF
CREATE USER GUI_XMDM_F7 IDENTIFIED BY password;
GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE TO GUI_XMDM_F7;
EOF
```

## Development

### Testing a JQ Filter

```bash
# Test ddl-to-derived-tables.jq on a single file
java -jar sequala-cli.jar parse oracle \
  --output "jq-file-sql:sql/oracle/ddl-to-derived-tables.jq" \
  --pretty true \
  DESIRED_STATE/GUI_XMDM_F7-PROD-tables.sql

# Test with bindings
java -jar sequala-cli.jar parse oracle \
  --output "jq-file-sql:sql/oracle/ddl-to-sync.jq" \
  --bind "schema=F7/schema.yaml" \
  --pretty true \
  DESIRED_STATE/GUI_XMDM_F7-PROD-tables.sql
```

### Debugging Schema Differences

```bash
# Get detailed diff
java -jar sequala-cli.jar plan \
  --source "DESIRED_STATE/GUI_XMDM_F7-PROD-tables.sql" \
  --database "jdbc:oracle:thin:system/orakel@//localhost:1521/FREEPDB1" \
  --schema "GUI_XMDM_F7" \
  --dialect oracle \
  --format sql \
  --source-transform "jq:sql/oracle/ddl-to-derived-tables.jq" \
  --pretty true

# Inspect database schema
java -jar sequala-cli.jar inspect \
  --database "jdbc:oracle:thin:system/orakel@//localhost:1521/FREEPDB1" \
  --schema "GUI_XMDM_F7" \
  --format json

# Note: The sequala CLI itself still requires full JDBC URLs.
# Only the oracle-comparison.sh wrapper uses the shorter connection string format.
```

## Contributing

When modifying JQ scripts:
1. Update this README if behavior changes
2. Test on multiple schemas (see `TODO.md` for test list)
3. Verify round-trip fidelity
4. Update `TODO.md` with results

## License

See main project LICENSE file.
