-- Unified table comparison between oracle-new (local) and oracle-orig (remote via orig_db link)
-- Usage: @compare.sql PATTERN [DATA_PATTERN]
--
-- PATTERN: Schema/table pattern for structure comparison (columns, constraints)
-- DATA_PATTERN: Schema/table pattern for data comparison (default: GUI_XMDM.XMDM_CONF%)
--               Use NONE to skip data comparison entirely
--
-- Pattern examples:
--   GUI_XMDM_F7.EUREX_REPO_KEY_FIGURES  - Single table (exact match)
--   GUI_XMDM_F7.%                        - All tables in schema
--   GUI_XMDM_F7.EUREX%                   - Tables matching prefix in schema
--   GUI_XMDM%                            - All tables in schemas matching pattern
--   %                                    - All tables in all schemas
--
-- Examples:
--   @compare.sql GUI_XMDM%                           - Uses default data pattern (GUI_XMDM.XMDM_CONF%)
--   @compare.sql GUI_XMDM% NONE                      - Structure only, no data comparison
--   @compare.sql GUI_XMDM_F7.% GUI_XMDM_F7.%         - Structure and data for all tables in schema

SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 200
SET PAGESIZE 1000
SET VERIFY OFF
SET DEFINE ON
SET FEEDBACK OFF

DEFINE pattern = &1
DEFINE data_pattern = &2

-- Parse pattern into schema and table parts
COLUMN schema_pattern NEW_VALUE schema_pattern NOPRINT
COLUMN table_pattern NEW_VALUE table_pattern NOPRINT

SELECT
  CASE
    WHEN INSTR('&pattern', '.') > 0 THEN SUBSTR('&pattern', 1, INSTR('&pattern', '.') - 1)
    ELSE '&pattern'
  END AS schema_pattern,
  CASE
    WHEN INSTR('&pattern', '.') > 0 THEN SUBSTR('&pattern', INSTR('&pattern', '.') + 1)
    ELSE '%'
  END AS table_pattern
FROM dual;

-- Parse data_pattern into schema and table parts
COLUMN data_schema_pattern NEW_VALUE data_schema_pattern NOPRINT
COLUMN data_table_pattern NEW_VALUE data_table_pattern NOPRINT

SELECT
  CASE
    WHEN '&data_pattern' = 'NONE' THEN 'NONE'
    WHEN INSTR('&data_pattern', '.') > 0 THEN SUBSTR('&data_pattern', 1, INSTR('&data_pattern', '.') - 1)
    ELSE '&data_pattern'
  END AS data_schema_pattern,
  CASE
    WHEN '&data_pattern' = 'NONE' THEN 'NONE'
    WHEN INSTR('&data_pattern', '.') > 0 THEN SUBSTR('&data_pattern', INSTR('&data_pattern', '.') + 1)
    ELSE '%'
  END AS data_table_pattern
FROM dual;

PROMPT
PROMPT ========================================================================
PROMPT COMPARING TABLES
PROMPT   Schema pattern: &schema_pattern
PROMPT   Table pattern:  &table_pattern
PROMPT   Data comparison: &data_schema_pattern.&data_table_pattern
PROMPT ========================================================================

-- =====================================================
-- TABLE INVENTORY
-- =====================================================
PROMPT
PROMPT === Table Inventory ===
COL owner FORMAT A25
COL table_name FORMAT A40
COL status FORMAT A30

WITH local_tables AS (
  SELECT owner, table_name
  FROM all_tables
  WHERE owner LIKE '&schema_pattern'
    AND table_name LIKE '&table_pattern'
    AND table_name NOT LIKE '%$%'
),
remote_tables AS (
  SELECT owner, table_name
  FROM all_tables@orig_db
  WHERE owner LIKE '&schema_pattern'
    AND table_name LIKE '&table_pattern'
    AND table_name NOT LIKE '%$%'
)
SELECT 
  COALESCE(l.owner, r.owner) AS owner,
  COALESCE(l.table_name, r.table_name) AS table_name,
  CASE
    WHEN r.table_name IS NULL THEN 'NEW (only in oracle-new)'
    WHEN l.table_name IS NULL THEN 'MISSING (only in oracle-orig)'
    ELSE 'EXISTS IN BOTH'
  END AS status
FROM local_tables l
FULL OUTER JOIN remote_tables r 
  ON l.owner = r.owner AND l.table_name = r.table_name
ORDER BY 
  CASE
    WHEN r.table_name IS NULL THEN 1
    WHEN l.table_name IS NULL THEN 2
    ELSE 3
  END,
  COALESCE(l.owner, r.owner),
  COALESCE(l.table_name, r.table_name);

-- =====================================================
-- SUMMARY COUNTS
-- =====================================================
PROMPT
PROMPT === Summary ===

WITH local_tables AS (
  SELECT owner, table_name
  FROM all_tables
  WHERE owner LIKE '&schema_pattern'
    AND table_name LIKE '&table_pattern'
    AND table_name NOT LIKE '%$%'
),
remote_tables AS (
  SELECT owner, table_name
  FROM all_tables@orig_db
  WHERE owner LIKE '&schema_pattern'
    AND table_name LIKE '&table_pattern'
    AND table_name NOT LIKE '%$%'
)
SELECT 
  COUNT(CASE WHEN r.table_name IS NULL THEN 1 END) AS new_tables,
  COUNT(CASE WHEN l.table_name IS NULL THEN 1 END) AS missing_tables,
  COUNT(CASE WHEN l.table_name IS NOT NULL AND r.table_name IS NOT NULL THEN 1 END) AS common_tables
FROM local_tables l
FULL OUTER JOIN remote_tables r 
  ON l.owner = r.owner AND l.table_name = r.table_name;

-- =====================================================
-- COLUMN DIFFERENCES (for tables that exist in both)
-- =====================================================
PROMPT
PROMPT === Column Differences (tables in both databases) ===
COL owner FORMAT A20
COL table_name FORMAT A30
COL column_name FORMAT A25
COL difference FORMAT A35
COL local_value FORMAT A15
COL remote_value FORMAT A15

WITH local_cols AS (
  SELECT t.owner, t.table_name, c.column_name, c.data_type, c.data_length, 
         c.data_precision, c.data_scale, c.nullable
  FROM all_tables t
  JOIN all_tab_columns c ON t.owner = c.owner AND t.table_name = c.table_name
  WHERE t.owner LIKE '&schema_pattern'
    AND t.table_name LIKE '&table_pattern'
    AND t.table_name NOT LIKE '%$%'
),
remote_cols AS (
  SELECT t.owner, t.table_name, c.column_name, c.data_type, c.data_length,
         c.data_precision, c.data_scale, c.nullable
  FROM all_tables@orig_db t
  JOIN all_tab_columns@orig_db c ON t.owner = c.owner AND t.table_name = c.table_name
  WHERE t.owner LIKE '&schema_pattern'
    AND t.table_name LIKE '&table_pattern'
    AND t.table_name NOT LIKE '%$%'
),
common_tables AS (
  SELECT l.owner, l.table_name
  FROM (SELECT DISTINCT owner, table_name FROM local_cols) l
  JOIN (SELECT DISTINCT owner, table_name FROM remote_cols) r
    ON l.owner = r.owner AND l.table_name = r.table_name
)
SELECT 
  COALESCE(l.owner, r.owner) AS owner,
  COALESCE(l.table_name, r.table_name) AS table_name,
  COALESCE(l.column_name, r.column_name) AS column_name,
  CASE
    WHEN r.column_name IS NULL THEN 'Column only in LOCAL'
    WHEN l.column_name IS NULL THEN 'Column only in REMOTE'
    WHEN l.data_type != r.data_type THEN 'Data type differs'
    WHEN NVL(l.data_length,0) != NVL(r.data_length,0) THEN 'Data length differs'
    WHEN NVL(l.data_precision,0) != NVL(r.data_precision,0) THEN 'Precision differs'
    WHEN NVL(l.data_scale,0) != NVL(r.data_scale,0) THEN 'Scale differs'
    WHEN l.nullable != r.nullable THEN 'Nullable differs'
  END AS difference,
  CASE
    WHEN r.column_name IS NULL THEN l.data_type
    WHEN l.column_name IS NULL THEN NULL
    WHEN l.data_type != r.data_type THEN l.data_type
    WHEN NVL(l.data_length,0) != NVL(r.data_length,0) THEN TO_CHAR(l.data_length)
    WHEN NVL(l.data_precision,0) != NVL(r.data_precision,0) THEN TO_CHAR(l.data_precision)
    WHEN NVL(l.data_scale,0) != NVL(r.data_scale,0) THEN TO_CHAR(l.data_scale)
    WHEN l.nullable != r.nullable THEN l.nullable
  END AS local_value,
  CASE
    WHEN r.column_name IS NULL THEN NULL
    WHEN l.column_name IS NULL THEN r.data_type
    WHEN l.data_type != r.data_type THEN r.data_type
    WHEN NVL(l.data_length,0) != NVL(r.data_length,0) THEN TO_CHAR(r.data_length)
    WHEN NVL(l.data_precision,0) != NVL(r.data_precision,0) THEN TO_CHAR(r.data_precision)
    WHEN NVL(l.data_scale,0) != NVL(r.data_scale,0) THEN TO_CHAR(r.data_scale)
    WHEN l.nullable != r.nullable THEN r.nullable
  END AS remote_value
FROM local_cols l
FULL OUTER JOIN remote_cols r 
  ON l.owner = r.owner AND l.table_name = r.table_name AND l.column_name = r.column_name
WHERE ((l.owner, l.table_name) IN (SELECT owner, table_name FROM common_tables)
    OR (r.owner, r.table_name) IN (SELECT owner, table_name FROM common_tables))
  AND (
    r.column_name IS NULL 
    OR l.column_name IS NULL
    OR l.data_type != r.data_type
    OR NVL(l.data_length,0) != NVL(r.data_length,0)
    OR NVL(l.data_precision,0) != NVL(r.data_precision,0)
    OR NVL(l.data_scale,0) != NVL(r.data_scale,0)
    OR l.nullable != r.nullable
  )
ORDER BY 1, 2, 3;

-- =====================================================
-- CONSTRAINT DIFFERENCES (for tables that exist in both)
-- =====================================================
PROMPT
PROMPT === Constraint Differences (tables in both databases) ===
COL owner FORMAT A20
COL table_name FORMAT A30
COL constraint_type FORMAT A5
COL columns FORMAT A30
COL difference FORMAT A25

WITH local_cons AS (
  SELECT c.owner, c.table_name, c.constraint_name, c.constraint_type,
         LISTAGG(cc.column_name, ',') WITHIN GROUP (ORDER BY cc.position) AS columns,
         c.search_condition_vc
  FROM all_constraints c
  LEFT JOIN all_cons_columns cc ON c.owner = cc.owner AND c.constraint_name = cc.constraint_name
  WHERE c.owner LIKE '&schema_pattern'
    AND c.table_name LIKE '&table_pattern'
    AND c.table_name NOT LIKE '%$%'
  GROUP BY c.owner, c.table_name, c.constraint_name, c.constraint_type, c.search_condition_vc
),
remote_cons AS (
  SELECT c.owner, c.table_name, c.constraint_name, c.constraint_type,
         LISTAGG(cc.column_name, ',') WITHIN GROUP (ORDER BY cc.position) AS columns,
         c.search_condition_vc
  FROM all_constraints@orig_db c
  LEFT JOIN all_cons_columns@orig_db cc ON c.owner = cc.owner AND c.constraint_name = cc.constraint_name
  WHERE c.owner LIKE '&schema_pattern'
    AND c.table_name LIKE '&table_pattern'
    AND c.table_name NOT LIKE '%$%'
  GROUP BY c.owner, c.table_name, c.constraint_name, c.constraint_type, c.search_condition_vc
),
common_tables AS (
  SELECT DISTINCT l.owner, l.table_name
  FROM local_cons l
  JOIN remote_cons r ON l.owner = r.owner AND l.table_name = r.table_name
)
SELECT 
  COALESCE(l.owner, r.owner) AS owner,
  COALESCE(l.table_name, r.table_name) AS table_name,
  COALESCE(l.constraint_type, r.constraint_type) AS constraint_type,
  COALESCE(l.columns, r.columns) AS columns,
  CASE
    WHEN r.constraint_type IS NULL THEN 'Only in LOCAL'
    WHEN l.constraint_type IS NULL THEN 'Only in REMOTE'
  END AS difference
FROM local_cons l
FULL OUTER JOIN remote_cons r 
  ON l.owner = r.owner 
  AND l.table_name = r.table_name
  AND l.constraint_type = r.constraint_type 
  AND NVL(l.columns, '_') = NVL(r.columns, '_')
  AND NVL(l.search_condition_vc, '_') = NVL(r.search_condition_vc, '_')
WHERE ((l.owner, l.table_name) IN (SELECT owner, table_name FROM common_tables)
    OR (r.owner, r.table_name) IN (SELECT owner, table_name FROM common_tables))
  AND (r.constraint_type IS NULL OR l.constraint_type IS NULL)
ORDER BY 1, 2, 3, 4;

-- =====================================================
-- DATA COMPARISON (using DBMS_COMPARISON for tables matching data pattern)
-- =====================================================
PROMPT
PROMPT === Data Comparison ===

SET SERVEROUTPUT ON

DECLARE
  v_scan_info DBMS_COMPARISON.COMPARISON_TYPE;
  v_consistent BOOLEAN;
  v_comparison_name VARCHAR2(128);
  v_count NUMBER := 0;
  v_identical NUMBER := 0;
  v_different NUMBER := 0;
  v_failed NUMBER := 0;
  v_data_schema_pattern VARCHAR2(128) := '&data_schema_pattern';
  v_data_table_pattern VARCHAR2(128) := '&data_table_pattern';
BEGIN
  IF v_data_schema_pattern = 'NONE' THEN
    DBMS_OUTPUT.PUT_LINE('Data comparison skipped (DATA_PATTERN = NONE)');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('To compare data, provide a second parameter:');
    DBMS_OUTPUT.PUT_LINE('  @compare.sql PATTERN DATA_PATTERN');
    DBMS_OUTPUT.PUT_LINE('  Example: @compare.sql GUI_XMDM% GUI_XMDM.XMDM_CONF%');
  ELSE
    DBMS_OUTPUT.PUT_LINE('Comparing data for: ' || v_data_schema_pattern || '.' || v_data_table_pattern);
    DBMS_OUTPUT.PUT_LINE('');

  FOR t IN (
    WITH local_tables AS (
      SELECT owner, table_name
      FROM all_tables
      WHERE owner LIKE v_data_schema_pattern
        AND table_name LIKE v_data_table_pattern
        AND table_name NOT LIKE '%$%'
    ),
    remote_tables AS (
      SELECT owner, table_name
      FROM all_tables@orig_db
      WHERE owner LIKE v_data_schema_pattern
        AND table_name LIKE v_data_table_pattern
        AND table_name NOT LIKE '%$%'
    )
    SELECT l.owner, l.table_name
    FROM local_tables l
    JOIN remote_tables r ON l.owner = r.owner AND l.table_name = r.table_name
    ORDER BY l.owner, l.table_name
  ) LOOP
    v_count := v_count + 1;
    v_comparison_name := 'CMP_' || SUBSTR(t.owner || '_' || t.table_name, 1, 120);
    
    BEGIN
      -- Drop existing comparison
      BEGIN
        DBMS_COMPARISON.DROP_COMPARISON(v_comparison_name);
      EXCEPTION
        WHEN OTHERS THEN NULL;
      END;
      
      -- Create and run comparison
      DBMS_COMPARISON.CREATE_COMPARISON(
        comparison_name    => v_comparison_name,
        schema_name        => t.owner,
        object_name        => t.table_name,
        dblink_name        => 'ORIG_DB',
        remote_schema_name => t.owner,
        remote_object_name => t.table_name
      );
      
      v_consistent := DBMS_COMPARISON.COMPARE(
        comparison_name => v_comparison_name,
        scan_info       => v_scan_info,
        perform_row_dif => TRUE
      );
      
      IF v_consistent THEN
        v_identical := v_identical + 1;
      ELSE
        v_different := v_different + 1;
        DBMS_OUTPUT.PUT_LINE('DIFFERENT: ' || t.owner || '.' || t.table_name);
      END IF;
      
    EXCEPTION
      WHEN OTHERS THEN
        v_failed := v_failed + 1;
        DBMS_OUTPUT.PUT_LINE('FAILED: ' || t.owner || '.' || t.table_name || ' - ' || SQLERRM);
    END;
  END LOOP;

  DBMS_OUTPUT.PUT_LINE('');
  DBMS_OUTPUT.PUT_LINE('Data comparison summary:');
  DBMS_OUTPUT.PUT_LINE('  Tables compared: ' || v_count);
  DBMS_OUTPUT.PUT_LINE('  Identical:       ' || v_identical);
  DBMS_OUTPUT.PUT_LINE('  Different:       ' || v_different);
  DBMS_OUTPUT.PUT_LINE('  Failed:          ' || v_failed);
  END IF; -- END IF for NONE check
END;
/

-- Show row differences for all comparisons (only if data comparison was run)
PROMPT
PROMPT === Row Differences (data comparison tables) ===
COL comparison FORMAT A50
COL index_value FORMAT A50
COL status FORMAT A20

SELECT
  SUBSTR(r.comparison_name, 5) AS comparison,
  r.index_value,
  CASE
    WHEN r.local_rowid IS NULL THEN 'Missing locally'
    ELSE 'Missing remotely'
  END AS status
FROM dba_comparison_row_dif r
WHERE r.comparison_name LIKE 'CMP_&data_schema_pattern%'
  AND '&data_schema_pattern' != 'NONE'
ORDER BY r.comparison_name, r.index_value;

PROMPT
PROMPT ========================================================================
PROMPT COMPARISON COMPLETE
PROMPT ========================================================================

EXIT;
