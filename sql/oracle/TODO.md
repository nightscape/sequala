# Oracle Comparison Pipeline - Issue Analysis

## Overview

The `oracle-comparison.sh` script compares the state of `oracle-orig` (populated from migration scripts) with the desired state (derived from the same scripts). In theory, this should produce minimal differences. In practice, ~1361 migration statements are generated.

## Pipeline Steps

1. **migrate-orig**: Parse and apply SQL migrations to `oracle-orig` using `--simplify` (filters noise, dedupes, sorts)
2. **generate-prod-tables**: Create desired state files in `DESIRED_STATE/` using `merge-ddl-and-metadata.jq`
3. **compare-desired-state**: Generate _AT/_HT tables with `ddl-to-derived-tables.jq`, then diff against `oracle-orig`

---

## Issues Found

### Issue 1: BIGINT Encoding Bug (Critical)

**Symptom**: Tables with `NUMBER(19)` columns fail to create in `oracle-orig`

**Example**:
```sql
-- Original SQL (F7/DEV/CREATE_XMDM_GUI_PRICE_TRADE_MONITOR_PARAMETERS.sql)
SPAN NUMBER(19) NOT NULL ENABLE

-- Generated in sorted-migrations.sql
SPAN BIGINT NOT NULL
```

**Root Cause**:
1. Oracle parser converts `NUMBER(19)` → `SqlBigInt` for round-trip consistency
   - File: `parser-oracle/.../OracleSQL.scala:1417`
2. When rendering via jq-file-sql, `CreateGenericTable` uses `GenericSqlRenderer`
   - File: `parser/.../ParserSqlRenderers.scala:380-387`
3. `GenericSqlRenderer` outputs `SqlBigInt` as `BIGINT` (ANSI SQL)
   - File: `schema/.../GenericSqlRenderer.scala:10`
4. Oracle doesn't support `BIGINT` → statement fails silently (ignored as syntax error)

**Affected Tables** (4 statements with BIGINT):
- `GUI_XMDM_REV_SHARING.EFS_CB012_DATA`
- `GUI_XMDM_REV_SHARING.EFS_CB012_DATA_AT`
- `GUI_XMDM_F7.PRICE_TRADE_MONITOR_PARAMETERS`
- `GUI_XMDM_F7.PRICE_TRADE_MONITOR_PARM_AT`

**Fix Location**: `OracleParserSqlRenderers.renderStatement` should handle `CreateGenericTable` using `OracleSqlRenderer` instead of falling through to `GenericSqlRenderer`.

---

### Issue 2: Missing _HT Tables in oracle-orig

**Symptom**: `compare-desired-state` generates many CREATE TABLE statements for _HT tables

**Example** (GUI_XMDM_REV_SHARING):
- oracle-orig has: 6 _HT tables
- Desired state generates: 38 _HT tables (one per base table)
- Migration: 38 CREATE TABLE statements for missing _HT

**Root Cause**:
1. Original SQL files are inconsistent - some tables have both _AT and _HT, others only _AT
2. `ddl-to-derived-tables.jq` generates BOTH _AT and _HT for ALL base tables
3. Mismatch creates many "missing" _HT tables

**Evidence**:
```bash
# Tables in oracle-orig GUI_XMDM_REV_SHARING with _HT suffix
OTC_IRD_MULTIPLIER_HT
OTC_IRD_MULTIPLIER_Q_PROV_HT
PARTICIPANTS_GROUPS_HT
PARTICIPANTS_GROUPS_REPO_HT
REPO_CLEARING_CORRECTIONS_HT
REPO_MULTIPLIER_HT
# Only 6 _HT tables, but 41 base tables exist
```

**Fix Options**:
1. Track which tables have _HT in original DDL (via metadata or comment annotation)
2. Modify `ddl-to-derived-tables.jq` to only generate _AT tables, not _HT
3. Add missing _HT tables to original SQL files for consistency

---

### Issue 3: Tables Without Metadata Excluded from Desired State

**Symptom**: `compare-desired-state` generates DROP TABLE for tables that exist in `oracle-orig`

**Example**:
```sql
-- Migration generated
DROP TABLE "GUI_XMDM_F7"."COMMENT_VALUES";
DROP TABLE "GUI_XMDM_F7"."COMMENT_VALUES_AT";
```

**Root Cause**:
1. `merge-ddl-and-metadata.jq` only includes tables that have `XMDM_CONF_TABLE` metadata entries
   - File: `merge-ddl-and-metadata.jq:211` - `select(($metadata | length) > 0)`
2. Tables like `COMMENT_VALUES` have DDL but no metadata
3. These tables are applied to `oracle-orig` (via `--simplify`) but excluded from desired state

**Affected Tables** (examples):
- `GUI_XMDM_F7.COMMENT_VALUES` (has DDL in DEV/, no metadata)
- Various tables in REV_SHARING schema

**Fix**: Modify `merge-ddl-and-metadata.jq` to:
- Include ALL tables in DDL output (not just those with metadata)
- Only generate YAML for tables with metadata

---

### Issue 4: Column Type Differences

**Symptom**: `ALTER TABLE MODIFY` statements in migrations

**Example**:
```sql
ALTER TABLE "GUI_XMDM_F7"."OWN_ISSUER_LIST" MODIFY ("ISSUER_ID" NUMBER(10) NOT NULL);
```

**Root Cause**: Type normalization differences between:
- What Oracle introspects from the database
- What the DDL files specify
- How types are encoded/decoded through JSON

**Impact**: Minor - these are usually no-ops if the types are equivalent

---

## Task List

### High Priority

- [ ] **Fix BIGINT encoding bug**
  - Modify `OracleParserSqlRenderers.renderStatement` to handle `CreateGenericTable`
  - Use `OracleSqlRenderer` for Oracle dialect output
  - Test: Re-run `migrate-orig` and verify `NUMBER(19)` columns work

- [ ] **Fix metadata-only filtering in merge-ddl-and-metadata.jq**
  - Include ALL CREATE TABLE statements in DDL output
  - Only filter for YAML generation (metadata file)
  - Test: Verify COMMENT_VALUES appears in F7-PROD-tables.sql

### Medium Priority

- [ ] **Add comment export to dump** (Issue 11)
  - Extend `OracleSchemaInspector` to query `ALL_TAB_COMMENTS` and `ALL_COL_COMMENTS`
  - Include `COMMENT ON` statements in DDL output
  - This will eliminate ~70 empty COMMENT migration statements

- [ ] **Handle _HT table generation**
  - Option A: Add annotation/comment to mark tables that have _HT
  - Option B: Only generate _AT in `ddl-to-derived-tables.jq`
  - Option C: Add missing _HT tables to original SQL files
  - Decide on approach based on business requirements

- [ ] **Re-run full pipeline after fixes**
  - Stop and restart containers: `./oracle-comparison.sh stop && ./oracle-comparison.sh start`
  - Run migrate-orig: `./oracle-comparison.sh migrate-orig`
  - Generate prod tables: `./oracle-comparison.sh generate-prod-tables --force`
  - Compare: `./oracle-comparison.sh compare-desired-state /tmp/migrations-fixed`
  - Verify migration count is significantly reduced

### Low Priority

- [x] **Investigate column type normalization** (DONE - 2025-12-15)
  - Documented type differences (Issues 5, 6, 7, 9, 10)
  - Added type equivalence rules to OracleDiffOptions:
    - `SqlTimestamp(6)` ≡ `SqlTimestamp(None)`
    - `Number(10, 0)` ≡ `SqlInteger`
    - `Number(p, 0)` ≡ `Number(p, None)`
  - Fixed CHAR_LENGTH vs DATA_LENGTH in inspector (Issue 6)
  - Fixed TBL_NAME_AT usage in ddl-to-derived-tables.jq (Issue 9)
  - Fixed PK columns implicitly NOT NULL in SchemaDiffer (Issue 10)

- [ ] **Add validation to pipeline**
  - Count tables before/after each step
  - Report on skipped/failed statements
  - Make migration errors more visible

- [ ] **Fix missing schema prefixes in source SQL** (Issue 8)
  - Add `GUI_XMDM_MMPM.` prefix to ~19 MMPRM tables
  - Or modify jq pipeline to inject schema from directory name

---

## Issue 5: INTEGER → NUMBER(10) Type Mismatch (FIXED)

**Symptom**: Spurious ALTER TABLE statements changing columns to NUMBER(10)

**Example**:
```sql
ALTER TABLE "GUI_XMDM_EPS_PRICE_LST"."KNOWN_PRICETYPES" MODIFY ("IS_PRIMARY" NUMBER(10));
```

**Root Cause**:
1. Original SQL: `INTEGER`
2. Parser: `INTEGER` → `SqlInteger`
3. Renderer (`schema-oracle/SqlRenderer.scala:54`): `SqlInteger` → `"NUMBER(10)"`
4. Applied to database as `NUMBER(10)` with `DATA_PRECISION=10`
5. When inspected from DB (`OracleSchemaInspector.scala:135`): `Number(Some(10), Some(0))`
6. Comparison: `SqlInteger` ≠ `Number(Some(10), Some(0))` → generates ALTER TABLE

**Fix Applied** (2025-12-15):
Added normalization in `migrate-oracle/.../OracleRunner.scala`:
```scala
case Number(Some(10), Some(0) | None) => SqlInteger
```
This treats `NUMBER(10, 0)` from database inspection as equivalent to `SqlInteger` from parsed DDL.

---

## Successful Process (2025-12-15)

### Step 1: Create Users/Schemas

Before running migrations, the database must have the users/schemas:

```bash
# Extract unique schema names from migrations
grep -o '"GUI_[^"]*"\.' sorted-migrations.sql | sort -u | sed 's/"//g; s/\.$//' > /tmp/schemas.txt

# Generate CREATE USER statements
while read user; do
  echo "CREATE USER $user IDENTIFIED BY password;"
  echo "GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE TO $user;"
done < /tmp/schemas.txt > /tmp/create-users.sql

# Execute on oracle-orig (port 1521)
cat /tmp/create-users.sql | docker exec -i oracle-orig sqlplus -s system/orakel@//localhost:1521/FREEPDB1
```

### Step 2: Run Migrations with scala-cli

```bash
# Use the run-migrations.sc script (NOT .scala)
scala-cli run sql/oracle/run-migrations.sc -- sorted-migrations.sql 2>&1 | tee /tmp/run-migrations.log
```

**Results (2025-12-15)**:
- Successful: 2034 statements
- Skipped: 20958 (ignorable errors like "already exists")
- Failed: 78 (PARALLEL 0 issue - minor)
- Tables created: 377

### Step 3: Compare DESIRED_STATE Against Database

To get ALTER TABLE statements (instead of CREATE TABLE), compare against a database that HAS the tables:

```bash
# Compare against oracle-orig (port 1521) which has the migrated tables
./sql/oracle/oracle-comparison.sh compare-desired-state migrations-against-orig \
  "jdbc:oracle:thin:system/orakel@//localhost:1521/FREEPDB1" 2>&1 | tee /tmp/compare-orig.log
```

**Output**: 461 migration statements (down from 544 when comparing against empty DB)

### Known Runtime Issues

1. **PARALLEL 0 error**: 78 tables with `PARALLEL 0` clause fail on Oracle Free
2. **SBT mode slow/stuck**: Use pre-built JAR or scala-cli instead of sbt for parsing large file sets

---

## Issue 6: VARCHAR2 CHAR Length Mismatch (FIXED)

**Symptom**: Spurious ALTER TABLE statements changing VARCHAR2 columns

**Example**:
```sql
ALTER TABLE "GUI_XMDM_CLOUDERA"."S_CLOUDERA_CONFIG" MODIFY ("SECTION" VARCHAR2(30 CHAR));
```

**Root Cause**:
1. Column defined as `VARCHAR2(30 CHAR)` in DDL
2. Oracle stores: `CHAR_LENGTH=30`, `DATA_LENGTH=120` (30×4 bytes for UTF-8), `CHAR_USED=C`
3. Inspector used `DATA_LENGTH` (120) instead of `CHAR_LENGTH` (30)
4. Created `Varchar2(120, Chars)` instead of `Varchar2(30, Chars)`
5. Differ saw 120 ≠ 30 → generated spurious ALTER TABLE

**Fix Applied** (2025-12-15):
Modified `migrate-oracle/.../OracleSchemaInspector.scala`:
- Added `char_length` to SQL query
- Use `char_length` when `CHAR_USED='C'`, `data_length` when `CHAR_USED='B'`

```scala
val effectiveLength = if charUsed == "C" then charLength.orElse(dataLength) else dataLength
```

---

## Issue 7: NUMBER Scale=0 Normalization (FIXED)

**Symptom**: Spurious ALTER TABLE for NUMBER columns with explicit scale

**Example**:
```sql
ALTER TABLE "GUI_XMDM_DMP"."S_STG_CAA_LIQUIDATION" MODIFY ("NOMINAL_AMOUNT" NUMBER(15));
```

**Root Cause**:
1. DDL specifies `NUMBER(15)` (no scale)
2. Oracle stores with `DATA_SCALE=0`
3. Inspector returns `Number(Some(15), Some(0))`
4. Parser returns `Number(Some(15), None)`
5. `Number(15, 0)` ≠ `Number(15, None)` → spurious ALTER

**Fix Applied** (2025-12-15):
Added normalization in `migrate-oracle/.../OracleRunner.scala`:
```scala
case Number(p, Some(0)) => Number(p, None)
```

---

## Issue 8: Tables Without Schema Prefix (Remaining)

**Symptom**: Tables created in SYSTEM schema instead of GUI_XMDM_* schema

**Example**:
```sql
-- DDL in source file (missing schema prefix)
CREATE TABLE "S_MMPRM_ENLIGHT_BLACK_LIST" (...)

-- Gets created in SYSTEM schema, not GUI_XMDM_MMPM
```

**Evidence**:
```
SYSTEM.S_MMPRM_ENLIGHT_BLACK_LIST
SYSTEM.S_MMPRM_OVERRULE
SYSTEM.S_MMPRM_PRODUCT_SETUP
-- etc. (19 tables in wrong schema)
```

**Root Cause**: Source SQL files in MMPM directory missing `GUI_XMDM_MMPM.` prefix

**Fix Required**: Add schema prefixes to source SQL files, or modify jq pipeline to inject schema.

---

## Issue 9: _AT Table Name from Metadata (FIXED)

**Symptom**: Generated _AT table names don't match actual names in DB

**Example**:
```sql
-- DB has:
S_STG_PAY_TRANSACT_MANUAL_AT  (truncated name)

-- Generated _AT:
S_STG_PAY_TRANSACTION_MANUAL_AT  (base name + "_AT")
```

**Root Cause**:
1. `ddl-to-derived-tables.jq` generated `base_table_name + "_AT"`
2. But XMDM_CONF_TABLE metadata has explicit `TBL_NAME_AT` with different name
3. YAML had this info but jq script didn't use it

**Fix Applied** (2025-12-15):
Modified `sql/oracle/ddl-to-derived-tables.jq`:
```jq
($metadata[$table_name_only] // {}) as $table_meta |
($table_meta.TBL_NAME_AT // ($table.name + "_AT")) as $at_table_name |
```
Now uses `TBL_NAME_AT` from metadata if available.

---

## Issue 10: PK Columns Spurious NOT NULL Diffs (FIXED)

**Symptom**: ALTER TABLE MODIFY for PK columns even though they match

**Example**:
```sql
-- Generated (spurious):
ALTER TABLE "GUI_XMDM_PUMPAPP"."DB_BLACKLIST" MODIFY ("DATABASE_NAME" VARCHAR2(128 BYTE));
```

**Root Cause**:
1. PK columns are implicitly NOT NULL in Oracle
2. DB returns `NULLABLE='N'` for PK columns
3. DDL doesn't always have explicit `NOT NULL` (implied by PK)
4. Differ saw `nullable=false` (DB) vs `nullable=true` (DDL) → spurious ALTER

**Fix Applied** (2025-12-15):
Modified `migrate/src/main/scala/sequala/migrate/SchemaDiffer.scala`:
- Collect PK column names from both tables
- Normalize `nullable=false` for PK columns before comparison

```scala
val fromPkCols = from.primaryKey.map(_.columns.map(normalize).toSet).getOrElse(Set.empty)
val toPkCols = to.primaryKey.map(_.columns.map(normalize).toSet).getOrElse(Set.empty)
// ...
fromNullable = if fromPkCols.contains(normalize(fromCol.name)) then false else fromCol.nullable
toNullable = if toPkCols.contains(normalize(toCol.name)) then false else toCol.nullable
```

---

## Progress Summary (2025-12-15)

| Run | Total Statements | Notes |
|-----|-----------------|-------|
| Initial | ~1361 | Before any fixes |
| After INTEGER fix (Issue 5) | 461 | NUMBER(10) ≡ SqlInteger |
| After CHAR length fix (Issue 6) | ~300 | Use CHAR_LENGTH for CHAR semantics |
| After scale=0 fix (Issue 7) | ~280 | NUMBER(p,0) ≡ NUMBER(p) |
| After TBL_NAME_AT fix (Issue 9) | ~260 | Use explicit _AT names from metadata |
| After PK nullable fix (Issue 10) | ~240 | PK columns implicitly NOT NULL |

**Current breakdown** (~240 statements):
- ~180 CREATE TABLE - mostly tables in wrong schema or missing _AT tables
- ~40 ALTER TABLE - ITS column differences, TIMESTAMP precision
- ~18 DROP TABLE - tables in DB not in DESIRED_STATE

**Remaining issues** (data/source problems, not differ bugs):
1. ~77 tables missing schema prefix in source SQL (Issue 8)
2. ~6 _AT tables missing ITS column (DB doesn't have it, jq adds it)
3. ~2 _AT tables with extra ITS column (DB has it, generated doesn't)
4. ~10 TIMESTAMP precision differences (TIMESTAMP(6) vs TIMESTAMP(9))
5. Various other source DDL inconsistencies

---

## Files Involved

| File | Purpose |
|------|---------|
| `oracle-comparison.sh` | Main orchestration script |
| `merge-ddl-and-metadata.jq` | Creates DESIRED_STATE files |
| `ddl-to-derived-tables.jq` | Generates _AT and _HT tables |
| `run-migrations.sc` | Scala CLI script to execute SQL against Oracle |

Note: `sort-and-filter.jq` has been replaced by the built-in `--simplify` flag in sequala.

| Scala File | Purpose |
|------------|---------|
| `parser-oracle/.../OracleSQL.scala` | Oracle SQL parser (NUMBER→SqlBigInt) |
| `parser/.../ParserSqlRenderers.scala` | Generic statement rendering |
| `parser-oracle/.../OracleParserSqlRenderers.scala` | Oracle-specific rendering |
| `schema/.../GenericSqlRenderer.scala` | ANSI SQL type rendering |
| `schema-oracle/.../SqlRenderer.scala` | Oracle SQL type rendering |

---

## Dump Command Issues (2025-12-15)

### New Approach: Dump from Database

Instead of parsing SQL files and merging with metadata, we now dump directly from the database:

```bash
./sql/oracle/oracle-comparison.sh dump-unified \
  "system/pass@//localhost:1521/FREEPDB1" \
  "GUI_XMDM%" \
  "GUI_XMDM.XMDM_CONF%"
```

This creates:
- `{schema}-PROD-tables.sql` - DDL for base tables (excluding _AT/_HT) with embedded metadata as comments
- Auto-generates column overrides for _AT/_HT tables

**Progress**: Reduced migration statements from 452 → 98

### Issue 11: Dump Does Not Export Comments

**Symptom**: Many `COMMENT ON ... IS ''` statements in migrations

**Example**:
```sql
COMMENT ON COLUMN "GUI_XMDM_F7"."COMMENT_VALUES"."COL_GUI_NAME" IS '';
```

**Root Cause**:
1. `OracleSchemaInspector` only fetches table structure, not comments
2. Dump exports `CREATE TABLE` without `COMMENT ON` statements
3. Metadata YAML has empty `COL_DESC` values
4. Comparison generates empty COMMENT statements

**Fix Required**: Extend `OracleSchemaInspector` to:
- Query `ALL_TAB_COMMENTS` for table comments
- Query `ALL_COL_COMMENTS` for column comments
- Include in dump output as `COMMENT ON TABLE/COLUMN` statements

---

### Issue 12: _HT Tables Without Metadata Not Generated

**Symptom**: `DROP TABLE` for _HT tables that exist in DB

**Example**:
```sql
DROP TABLE "GUI_XMDM_F7"."OWN_ISSUER_LIST_HT";
```

**Investigation**:
```
DB has:
  - OWN_ISSUER_LIST (base table) ✓
  - OWN_ISSUER_LIST_AT ✓
  - OWN_ISSUER_LIST_HT ✓

Metadata: NOT in XMDM_CONF (no entry at all)
```

**Root Cause**:
1. Base table `OWN_ISSUER_LIST` exists but has no XMDM_CONF metadata entry
2. `ddl-to-derived-tables.jq` only generates _HT when `HISTORICAL_TBL_FLAG=Y` in metadata
3. Since no metadata exists, no _HT is generated
4. Migration wants to DROP the existing _HT table

**Current Behavior**: Correct - only tables with `HISTORICAL_TBL_FLAG=Y` should have _HT generated.
The DROP is expected because the DB has _HT tables that shouldn't exist per the metadata rules.

---

### Issue 13: Orphaned _AT Tables (No Base Table)

**Symptom**: `DROP TABLE` for _AT tables in DB, `CREATE TABLE` for expected _AT tables

**Example** (GUI_XMDM_REV_SHARING):
```sql
-- These _AT tables exist in DB but their base tables don't:
DROP TABLE "GUI_XMDM_REV_SHARING"."F_REV_SHA_CAPITALLAB_DATA_AT";
DROP TABLE "GUI_XMDM_REV_SHARING"."F_REV_SHA_VOLMATCH_DATA_AT";

-- This base table exists, so _AT should be created:
CREATE TABLE "GUI_XMDM_REV_SHARING"."D_CREDIT_MEMBER_ADDRESSES_AT" ...;
```

**Investigation**:
```
F_REV_SHA_CAPITALLAB_DATA_AT exists in DB
F_REV_SHA_VOLMATCH_DATA_AT exists in DB
BUT: Base tables (F_REV_SHA_CAPITALLAB_DATA, F_REV_SHA_VOLMATCH_DATA) do NOT exist

D_CREDIT_MEMBER_ADDRESSES exists in DB (base table)
D_CREDIT_MEMBER_ADDRESSES_AT does NOT exist in DB
```

**Root Cause**:
1. Some _AT tables were created historically but their base tables were later deleted
2. Metadata still references these tables via `TBL_NAME_AT`
3. The dump only exports existing tables, not orphaned _AT tables

**Impact**: These are legitimate data issues in the source database, not tool bugs.

---

### Current Migration Breakdown (98 statements)

After dump-based comparison against oracle-orig:

| Category | Count | Examples |
|----------|-------|----------|
| Empty COMMENT | ~70 | `COMMENT ON COLUMN ... IS ''` |
| DROP _HT | ~5 | Tables with no metadata |
| CREATE _AT | ~5 | Base tables missing _AT |
| ALTER TABLE | ~15 | Column type/nullability differences |
| DROP _AT (orphaned) | ~3 | _AT without base table |

---

## Round-Trip Test Results (2025-12-16)

### Test Methodology

1. **Dump**: Export all GUI_XMDM schemas using `dump-unified` with `unified-dump-filter.jq`
2. **Compare**: Run `plan` with `--source-transform ddl-to-derived-tables.jq` against same database
3. **Pass criteria**: Zero migration statements generated

### Results: 18/32 schemas pass (56%)

| Schema | Status | Issue |
|--------|--------|-------|
| GUI_XMDM_CLOUDERA | ✓ Pass | |
| GUI_XMDM_CST | ✓ Pass | |
| GUI_XMDM_DATA_PLATFORM | ✓ Pass | |
| GUI_XMDM_DMP | ✓ Pass | |
| GUI_XMDM_DTCC | ✓ Pass | |
| GUI_XMDM_EPS_PRICE_LST | ✓ Pass | |
| GUI_XMDM_EPS_REBATES_LP | ✓ Pass | |
| GUI_XMDM_EPS_REBATES_V | ✓ Pass | |
| GUI_XMDM_ESMIRA | ✗ Fail | ITS type: `TIMESTAMP DEFAULT CURRENT_TIMESTAMP` vs `DATE DEFAULT SYSDATE` |
| GUI_XMDM_EUREX_HFT | ✓ Pass | |
| GUI_XMDM_F7_HFT | ✓ Pass | |
| GUI_XMDM_F7 | ✗ Fail | DROP _HT tables, empty COMMENTs |
| GUI_XMDM_LST | ✓ Pass | |
| GUI_XMDM_MBSP | ✓ Pass | |
| GUI_XMDM_MMPM | ✗ Fail | DROP _HT, CREATE _AT for new tables |
| GUI_XMDM_OTC | ✓ Pass | |
| GUI_XMDM_PMBFZ | ✗ Fail | _HT nullable differences |
| GUI_XMDM_PMDS | ✓ Pass | |
| GUI_XMDM_PMETF | ✗ Fail | _HT nullable differences |
| GUI_XMDM_PMRLP | ✓ Pass | |
| GUI_XMDM_PMRMM | ✓ Pass | |
| GUI_XMDM_PUMPAPP | ✗ Fail | ITS type mismatch |
| GUI_XMDM_REV_SHARING | ✗ Fail | Orphaned _AT tables, missing _AT |
| GUI_XMDM_RLI | ✗ Fail | _HT nullable differences |
| GUI_XMDM_RRS | ✗ Fail | Complex _AT naming (`ECAG_TRN_AT_NEW_AT`) |
| GUI_XMDM_SCS | ✗ Fail | ITS type mismatch |
| GUI_XMDM_TBCR | ✗ Fail | ITS type mismatch |
| GUI_XMDM_TEST | ✗ Fail | Missing _AT in DB |
| GUI_XMDM_XETRA_HFT | ✗ Fail | DROP orphaned _AT |
| GUI_XMDM_XETRA_HYPMI | ✓ Pass | |
| GUI_XMDM_XETRA_RTS24 | ✓ Pass | |
| GUI_XMDM | ✗ Fail | Constraint differences |

### Failure Categories

| Issue Type | Count | Examples |
|------------|-------|----------|
| ITS column type mismatch | 5 | ESMIRA, PUMPAPP, SCS, TBCR - DB has `TIMESTAMP DEFAULT CURRENT_TIMESTAMP` but generated expects `DATE DEFAULT SYSDATE` |
| _HT table discrepancies | 5 | F7, MMPM, PMBFZ, PMETF, RLI - DROP/CREATE _HT that shouldn't exist or are missing |
| Missing _AT in DB | 2 | TEST, RRS - Tables exist in DDL but no _AT table in database |
| Orphaned _AT tables | 2 | REV_SHARING, XETRA_HFT - _AT exists but base table deleted |
| Complex naming | 1 | RRS - `ECAG_TRN_AT_NEW_AT` (table ending in _AT gets _AT suffix) |
| Constraint diffs | 1 | GUI_XMDM - system-generated constraint names |

### Features Implemented (2025-12-16)

- [x] **@bind directive for source-transform** - Load YAML metadata into JQ filter
- [x] **TBL_NAME_AT support** - Use custom _AT table names from metadata
- [x] **Column skip override** - `skip: true` to exclude columns from _AT generation
- [x] **Column dataType override** - Detect and record dataType differences in YAML
- [x] **Column nullable override** - Detect and record nullable differences in YAML
- [x] **Schema pattern support** - `--schema "GUI_XMDM%"` now supports SQL LIKE patterns (%, _)
  - Works for `plan`, `apply`, `inspect`, `dump` commands
  - Oracle: Uses `OracleSchemaInspector.getSchemaNames` to resolve matching schemas
  - Enables processing multiple schemas in a single command

### Remaining Issues

1. **ITS column type inconsistency** (Issue 14)
   - Some tables use `TIMESTAMP DEFAULT CURRENT_TIMESTAMP`
   - Others use `DATE DEFAULT SYSDATE`
   - Need to detect ITS type in dump and add override

2. **_HT table existence tracking** (Issue 12)
   - `HISTORICAL_TBL_FLAG=Y` doesn't always match actual _HT existence
   - Need to track actual _HT tables in database

3. **Complex _AT naming patterns** (Issue 15)
   - Tables ending in `_AT` get `_AT_AT` suffix
   - Need special handling or explicit TBL_NAME_AT

---

## Verification Commands

```bash
# Check BIGINT occurrences in sorted migrations
grep "BIGINT" sorted-migrations.sql

# Count tables in oracle-orig
echo "SELECT owner, COUNT(*) FROM all_tables WHERE owner LIKE 'GUI_XMDM%' GROUP BY owner;" | \
  docker exec -i oracle-orig sqlplus -s system/orakel@//localhost/FREEPDB1

# List _HT tables in a schema
echo "SELECT table_name FROM all_tables WHERE owner = 'GUI_XMDM_REV_SHARING' AND table_name LIKE '%_HT';" | \
  docker exec -i oracle-orig sqlplus -s system/orakel@//localhost/FREEPDB1

# Count statements in migration output
grep -c ';$' /private/tmp/migrations-full/*.sql | tail -5

# Run round-trip test on all schemas (using schema pattern)
./sql/oracle/oracle-comparison.sh dump-unified 'jdbc:oracle:thin:system/orakel@//localhost:1521/FREEPDB1' 'GUI_XMDM%' 'GUI_XMDM.XMDM_CONF%'

# Compare all GUI_XMDM schemas at once using schema pattern
java -jar cli/target/scala-3.3.6/sequala-cli-*.jar plan \
  --source "sql/oracle/DESIRED_STATE/*-PROD-tables.sql" \
  --database "jdbc:oracle:thin:system/orakel@//localhost:1521/FREEPDB1" \
  --schema "GUI_XMDM%" \
  --dialect oracle --format sql \
  --source-transform "jq:sql/oracle/ddl-to-derived-tables.jq" \
  --write-to migrations/

# Or use the helper script
./sql/oracle/oracle-comparison.sh compare-desired-state migrations \
  "jdbc:oracle:thin:system/orakel@//localhost:1521/FREEPDB1" "GUI_XMDM%"
```
