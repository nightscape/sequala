#!/bin/bash
set -e

NETWORK_NAME="oracle-comparison-net"
ORIG_CONTAINER="oracle-orig"
NEW_CONTAINER="oracle-new"
ORACLE_PASSWORD="${ORACLE_PASSWORD:-orakel}"
APP_USER="${APP_USER:-xmdm_user}"
APP_USER_PASSWORD="${APP_USER_PASSWORD:-xmdm_pass}"

# Default target instance (can be overridden)
# Connection string format: user/pass@//host:port/service
DEFAULT_CONN_STRING="${DEFAULT_CONN_STRING:-system/$ORACLE_PASSWORD@//localhost:1521/FREEPDB1}"
DEFAULT_CONTAINER="${DEFAULT_CONTAINER:-$ORIG_CONTAINER}"

# Parse connection string components
# Format: user/pass@//host:port/service
parse_conn_string() {
  local conn="$1"
  local component="$2"

  case "$component" in
    user)
      echo "$conn" | sed -n 's|\([^/]*\)/.*|\1|p'
      ;;
    password)
      echo "$conn" | sed -n 's|[^/]*/\([^@]*\)@.*|\1|p'
      ;;
    host)
      echo "$conn" | sed -n 's|.*@//\([^:]*\):.*|\1|p'
      ;;
    port)
      echo "$conn" | sed -n 's|.*@//[^:]*:\([0-9]*\)/.*|\1|p'
      ;;
    service)
      echo "$conn" | sed -n 's|.*@//[^/]*/\(.*\)|\1|p'
      ;;
  esac
}

# Convert connection string to full JDBC URL
to_jdbc_url() {
  local conn="${1:-$DEFAULT_CONN_STRING}"
  echo "jdbc:oracle:thin:$conn"
}

# Get sqlplus connection string for use inside a container (connects to localhost)
get_sqlplus_conn() {
  local conn="${1:-$DEFAULT_CONN_STRING}"
  local user password service
  user=$(parse_conn_string "$conn" user)
  password=$(parse_conn_string "$conn" password)
  service=$(parse_conn_string "$conn" service)
  echo "${user}/${password}@//localhost/${service}"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Command to run sequala CLI - override with SEQUALA_CMD env var
# Examples:
#   SEQUALA_CMD="sbt 'cli/run"   (note: trailing quote added by script)
#   SEQUALA_CMD="java -jar sequala.jar"
# For sbt, the script wraps arguments in quotes to handle 'sbt cli/run arg1 arg2'
# Default: use assembly JAR with sufficient memory
SEQUALA_JAR="${SEQUALA_JAR:-$PROJECT_ROOT/cli/target/scala-3.3.6/sequala-cli-2.0.0-M6.jar}"
SEQUALA_MEMORY="${SEQUALA_MEMORY:--Xmx8g -Xms4g}"
SEQUALA_CMD="${SEQUALA_CMD:-}"
SEQUALA_SBT_MODE="${SEQUALA_SBT_MODE:-auto}"  # auto, true, or false

wait_for_oracle() {
  local container=$1
  local conn="${2:-$DEFAULT_CONN_STRING}"
  local max_attempts=60
  local attempt=1
  local sqlplus_conn
  sqlplus_conn=$(get_sqlplus_conn "$conn")

  echo "Waiting for $container to be ready..."
  while [ $attempt -le $max_attempts ]; do
    if echo "SELECT 1 FROM dual;" | docker exec -i "$container" sqlplus -s "$sqlplus_conn" 2>/dev/null | grep -q "1"; then
      echo "$container is ready!"
      return 0
    fi
    echo "  Attempt $attempt/$max_attempts - waiting..."
    sleep 5
    attempt=$((attempt + 1))
  done

  echo "ERROR: $container did not become ready in time"
  return 1
}

wait_for_cross_container() {
  local from_container=$1
  local to_container=$2
  local conn="${3:-$DEFAULT_CONN_STRING}"
  local max_attempts=30
  local attempt=1
  local user password service
  user=$(parse_conn_string "$conn" user)
  password=$(parse_conn_string "$conn" password)
  service=$(parse_conn_string "$conn" service)

  echo "Waiting for $to_container to be reachable from $from_container..."
  while [ $attempt -le $max_attempts ]; do
    if echo "SELECT 1 FROM dual;" | docker exec -i "$from_container" sqlplus -s "${user}/${password}@//${to_container}:1521/${service}" 2>/dev/null | grep -q "1"; then
      echo "  $to_container is reachable from $from_container!"
      return 0
    fi
    echo "  Attempt $attempt/$max_attempts - waiting..."
    sleep 2
    attempt=$((attempt + 1))
  done

  echo "ERROR: $to_container not reachable from $from_container in time"
  return 1
}

run_sql() {
  local container=$1
  local sql=$2
  local conn="${3:-$DEFAULT_CONN_STRING}"
  local max_attempts=10
  local attempt=1
  local sqlplus_conn
  sqlplus_conn=$(get_sqlplus_conn "$conn")

  while [ $attempt -le $max_attempts ]; do
    local output
    output=$(echo "$sql" | docker exec -i "$container" sqlplus -s "$sqlplus_conn" 2>&1)
    if ! echo "$output" | grep -q "^ERROR:\|^ORA-\|^SP2-"; then
      echo "$output"
      return 0
    fi
    echo "  Attempt $attempt/$max_attempts failed, retrying in 2s..."
    sleep 2
    attempt=$((attempt + 1))
  done

  echo "ERROR: Failed to execute SQL after $max_attempts attempts"
  echo "$output"
  return 1
}

cmd_start() {
  echo "=== Creating Docker network ==="
  docker network create "$NETWORK_NAME" 2>/dev/null || echo "Network already exists"

  echo "=== Starting $ORIG_CONTAINER ==="
  docker run -d \
    --name "$ORIG_CONTAINER" \
    --network "$NETWORK_NAME" \
    -p 1521:1521 \
    -e APP_USER="$APP_USER" \
    -e APP_USER_PASSWORD="$APP_USER_PASSWORD" \
    -e ORACLE_PASSWORD="$ORACLE_PASSWORD" \
    gvenzl/oracle-free:slim-faststart

  echo "=== Starting $NEW_CONTAINER ==="
  docker run -d \
    --name "$NEW_CONTAINER" \
    --network "$NETWORK_NAME" \
    -p 1522:1521 \
    -v "$SCRIPT_DIR:/scripts:ro" \
    -e APP_USER="$APP_USER" \
    -e APP_USER_PASSWORD="$APP_USER_PASSWORD" \
    -e ORACLE_PASSWORD="$ORACLE_PASSWORD" \
    gvenzl/oracle-free:slim-faststart

  echo "=== Waiting for databases to be ready ==="
  wait_for_oracle "$ORIG_CONTAINER"
  wait_for_oracle "$NEW_CONTAINER"

  echo "=== Waiting for cross-container connectivity ==="
  wait_for_cross_container "$ORIG_CONTAINER" "$NEW_CONTAINER"
  wait_for_cross_container "$NEW_CONTAINER" "$ORIG_CONTAINER"

  local service
  service=$(parse_conn_string "$DEFAULT_CONN_STRING" service)

  echo "=== Creating database link on $ORIG_CONTAINER (to $NEW_CONTAINER) ==="
  run_sql "$ORIG_CONTAINER" "CREATE DATABASE LINK new_db CONNECT TO system IDENTIFIED BY $ORACLE_PASSWORD USING '$NEW_CONTAINER:1521/$service';"

  echo "Testing link..."
  run_sql "$ORIG_CONTAINER" "SELECT * FROM dual@new_db;"

  echo "=== Creating database link on $NEW_CONTAINER (to $ORIG_CONTAINER) ==="
  run_sql "$NEW_CONTAINER" "CREATE DATABASE LINK orig_db CONNECT TO system IDENTIFIED BY $ORACLE_PASSWORD USING '$ORIG_CONTAINER:1521/$service';"

  echo "Testing link..."
  run_sql "$NEW_CONTAINER" "SELECT * FROM dual@orig_db;"

  echo ""
  echo "=== Setup complete ==="
  echo ""
  echo "Connection details:"
  echo "  $ORIG_CONTAINER: localhost:1521/$service (user: $APP_USER)"
  echo "  $NEW_CONTAINER:  localhost:1522/$service (user: $APP_USER)"
  echo ""
  echo "Database links:"
  echo "  From $ORIG_CONTAINER: SELECT * FROM table@new_db;"
  echo "  From $NEW_CONTAINER:  SELECT * FROM table@orig_db;"
}

cmd_stop() {
  echo "=== Stopping containers ==="
  docker stop "$ORIG_CONTAINER" "$NEW_CONTAINER" 2>/dev/null || true

  echo "=== Removing containers ==="
  docker rm "$ORIG_CONTAINER" "$NEW_CONTAINER" 2>/dev/null || true

  echo "=== Removing network ==="
  docker network rm "$NETWORK_NAME" 2>/dev/null || true

  echo "=== Cleanup complete ==="
}

cmd_status() {
  echo "=== Container status ==="
  docker ps -a --filter "name=$ORIG_CONTAINER" --filter "name=$NEW_CONTAINER" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
}

cmd_start_single() {
  local container="${1:-$NEW_CONTAINER}"
  local port="${2:-1522}"

  echo "=== Creating Docker network ==="
  docker network create "$NETWORK_NAME" 2>/dev/null || echo "Network already exists"

  echo "=== Starting $container ==="
  docker run -d \
    --name "$container" \
    --network "$NETWORK_NAME" \
    -p "$port":1521 \
    -v "$SCRIPT_DIR:/scripts:ro" \
    -e APP_USER="$APP_USER" \
    -e APP_USER_PASSWORD="$APP_USER_PASSWORD" \
    -e ORACLE_PASSWORD="$ORACLE_PASSWORD" \
    gvenzl/oracle-free:slim-faststart

  echo "=== Waiting for database to be ready ==="
  local service
  service=$(parse_conn_string "$DEFAULT_CONN_STRING" service)
  local conn="system/$ORACLE_PASSWORD@//localhost:$port/$service"
  wait_for_oracle "$container" "$conn"

  echo ""
  echo "=== Setup complete ==="
  echo ""
  echo "Connection details:"
  echo "  $container: localhost:$port/$service (user: $APP_USER)"
}

cmd_create_link() {
  local link_name="$1"
  local target_conn="$2"
  local container="${3:-$NEW_CONTAINER}"
  local source_conn="${4:-$DEFAULT_CONN_STRING}"

  if [ -z "$link_name" ] || [ -z "$target_conn" ]; then
    echo "Usage: $0 create-link <link-name> <target-conn> [container] [source-conn]"
    echo ""
    echo "Create a database link from container to target database."
    echo ""
    echo "Arguments:"
    echo "  link-name    Name for the database link (e.g., staging_db, prod_db)"
    echo "  target-conn  Target connection: user/pass@//host:port/service"
    echo "  container    Docker container (default: $NEW_CONTAINER)"
    echo "  source-conn  Connection inside container (default: $DEFAULT_CONN_STRING)"
    echo ""
    echo "Examples:"
    echo "  $0 create-link staging_db system/pass@//staging:1521/ORCL"
    echo "  $0 create-link prod_db system/pass@//prod:1521/ORCL oracle-new"
    exit 1
  fi

  local user password host port service
  user=$(parse_conn_string "$target_conn" user)
  password=$(parse_conn_string "$target_conn" password)
  host=$(parse_conn_string "$target_conn" host)
  port=$(parse_conn_string "$target_conn" port)
  service=$(parse_conn_string "$target_conn" service)

  echo "=== Creating database link '$link_name' on $container ==="
  echo "  Target: $host:$port/$service"

  run_sql "$container" \
    "CREATE DATABASE LINK $link_name CONNECT TO $user IDENTIFIED BY $password USING '$host:$port/$service';" \
    "$source_conn"

  echo "Testing link..."
  run_sql "$container" "SELECT * FROM dual@$link_name;" "$source_conn"
}

# Prepare a container for XMDM operations:
# - Create required tablespaces
# - Grant quotas to schemas
# - Widen XMDM_CONF columns to handle long schema names
prepare_container() {
  local container="$1"
  local conn="${2:-$DEFAULT_CONN_STRING}"
  local sqlplus_conn
  sqlplus_conn=$(get_sqlplus_conn "$conn")

  echo "=== Preparing container $container ==="

  # Create tablespaces that may be referenced in DDL
  local tablespaces=("XMDM_FACT" "EUREX_FACT" "FACT_DATA")
  for ts in "${tablespaces[@]}"; do
    echo "  Creating tablespace $ts..."
    echo "CREATE TABLESPACE $ts DATAFILE '${ts}.dbf' SIZE 100M AUTOEXTEND ON;" | \
      docker exec -i "$container" sqlplus -s "$sqlplus_conn" 2>/dev/null || true
  done

  # Grant unlimited quota on all tablespaces to common schemas
  echo "  Granting tablespace quotas..."
  echo "
ALTER USER GUI_XMDM QUOTA UNLIMITED ON USERS;
ALTER USER GUI_XMDM QUOTA UNLIMITED ON XMDM_FACT;
ALTER USER GUI_XMDM QUOTA UNLIMITED ON EUREX_FACT;
ALTER USER GUI_XMDM QUOTA UNLIMITED ON FACT_DATA;
ALTER USER GUI_XMDM DEFAULT TABLESPACE USERS;
" | docker exec -i "$container" sqlplus -s "$sqlplus_conn" 2>/dev/null || true

  # Widen XMDM_CONF columns to handle long schema names (e.g., GUI_XMDM_EPS_REBATES_LP = 24 chars)
  echo "  Widening XMDM_CONF columns..."
  echo "
ALTER TABLE GUI_XMDM.XMDM_CONF_TABLE MODIFY APP_NAME VARCHAR2(50);
ALTER TABLE GUI_XMDM.XMDM_CONF_TABLE MODIFY TBL_NAME_AT VARCHAR2(64);
ALTER TABLE GUI_XMDM.XMDM_CONF_COLUMN MODIFY APP_NAME VARCHAR2(50);
ALTER TABLE GUI_XMDM.XMDM_CONF_COLUMN MODIFY COL_NAME VARCHAR2(61);
ALTER TABLE GUI_XMDM.XMDM_CONF_COLUMN MODIFY COL_VALID_INTERVAL_COLUMN VARCHAR2(61);
" | docker exec -i "$container" sqlplus -s "$sqlplus_conn" 2>/dev/null || true

  echo "  Container prepared"
}

cmd_apply_desired_state() {
  local container="${1:-$ORIG_CONTAINER}"
  local conn="${2:-$DEFAULT_CONN_STRING}"
  local desired_state_dir="${3:-$SCRIPT_DIR/DESIRED_STATE}"

  if [ ! -d "$desired_state_dir" ]; then
    echo "ERROR: DESIRED_STATE directory not found: $desired_state_dir"
    exit 1
  fi

  local service
  service=$(parse_conn_string "$conn" service)
  local sqlplus_conn
  sqlplus_conn=$(get_sqlplus_conn "$conn")

  echo "=== Applying DESIRED_STATE to $container ==="
  echo "  Source: $desired_state_dir"
  echo "  Connection: $conn"
  echo ""

  # Step 1: Combine all DDL files
  echo "=== Step 1: Combining DDL files ==="
  local ddl_file="/tmp/desired-state-ddl-$$.sql"
  cat "$desired_state_dir"/*-PROD-tables.sql > "$ddl_file"
  echo "  Combined $(wc -l < "$ddl_file" | tr -d ' ') lines, $(grep -c ';$' "$ddl_file") statements"

  # Step 2: Apply DDL (creates tables)
  echo ""
  echo "=== Step 2: Applying DDL ==="
  run_migrations "$ddl_file" "$container" "$conn"

  # Step 3: Prepare container (tablespaces, quotas, column widening)
  echo ""
  echo "=== Step 3: Preparing container ==="
  prepare_container "$container" "$conn"

  # Step 4: Generate metadata INSERTs using ddl-to-sync.jq
  echo ""
  echo "=== Step 4: Generating metadata INSERTs ==="
  local metadata_dir="/tmp/metadata-inserts-$$"
  mkdir -p "$metadata_dir"
  cd "$PROJECT_ROOT"

  # shellcheck disable=SC2086
  run_sequala parse oracle \
    --output "jq-file-sql:$SCRIPT_DIR/ddl-to-sync.jq" \
    --write-to "$metadata_dir/" \
    "$desired_state_dir"/*-PROD-tables.sql 2>&1 | grep -E "^(Wrote:|Warning:)" || true

  # Step 5: Combine and apply metadata INSERTs
  echo ""
  echo "=== Step 5: Applying metadata INSERTs ==="
  local metadata_file="/tmp/all-metadata-$$.sql"
  cat "$metadata_dir"/*-metadata.sql > "$metadata_file" 2>/dev/null || true

  if [ -f "$metadata_file" ] && [ -s "$metadata_file" ]; then
    local insert_count
    insert_count=$(grep -c ';$' "$metadata_file" || echo 0)
    echo "  Applying $insert_count INSERT statements..."

    local error_count
    error_count=$(docker exec -i "$container" sqlplus -s "$sqlplus_conn" < "$metadata_file" 2>&1 | grep -cE "^(ERROR|ORA-)" || echo 0)
    echo "COMMIT;" | docker exec -i "$container" sqlplus -s "$sqlplus_conn" >/dev/null 2>&1

    echo "  Completed with $error_count errors"
  else
    echo "  No metadata INSERTs generated"
  fi

  # Step 6: Report final state
  echo ""
  echo "=== Step 6: Final state ==="
  echo "
SELECT 'Tables' as object_type, COUNT(*) as cnt FROM all_tables WHERE owner LIKE 'GUI_XMDM%'
UNION ALL
SELECT 'XMDM_CONF_TABLE rows', COUNT(*) FROM GUI_XMDM.XMDM_CONF_TABLE
UNION ALL
SELECT 'XMDM_CONF_COLUMN rows', COUNT(*) FROM GUI_XMDM.XMDM_CONF_COLUMN;
" | docker exec -i "$container" sqlplus -s "$sqlplus_conn"

  # Cleanup
  rm -f "$ddl_file" "$metadata_file"
  rm -rf "$metadata_dir"

  echo ""
  echo "=== DESIRED_STATE applied to $container ==="
}

cmd_roundtrip_test() {
  local desired_state_dir="${1:-$SCRIPT_DIR/DESIRED_STATE}"
  local compare_pattern="${2:-GUI_XMDM%}"
  local compare_data_pattern="${3:-GUI_XMDM.XMDM_CONF%}"

  echo "=== Roundtrip Test: DESIRED_STATE -> oracle-orig -> dump -> oracle-new -> compare ==="
  echo "  Source: $desired_state_dir"
  echo "  Compare pattern: $compare_pattern"
  echo "  Data pattern: $compare_data_pattern"
  echo ""

  local service
  service=$(parse_conn_string "$DEFAULT_CONN_STRING" service)
  local orig_conn="system/$ORACLE_PASSWORD@//localhost:1521/$service"
  local new_conn="system/$ORACLE_PASSWORD@//localhost:1522/$service"
  local new_sqlplus_conn
  new_sqlplus_conn=$(get_sqlplus_conn "$new_conn")

  # Step 1: Ensure containers are running
  echo "=== Step 1: Ensuring containers are running ==="
  if ! docker ps --format '{{.Names}}' | grep -q "^${ORIG_CONTAINER}$"; then
    echo "Starting containers..."
    cmd_start
  else
    echo "Containers already running"
  fi

  # Step 2: Apply DESIRED_STATE to oracle-orig
  echo ""
  echo "=== Step 2: Applying DESIRED_STATE to oracle-orig ==="
  cmd_apply_desired_state "$ORIG_CONTAINER" "$orig_conn" "$desired_state_dir"

  # Step 3: Prepare oracle-new
  echo ""
  echo "=== Step 3: Preparing oracle-new ==="
  prepare_container "$NEW_CONTAINER" "$new_conn"

  # Step 4: Run roundtrip (dump from oracle-orig, apply to oracle-new)
  echo ""
  echo "=== Step 4: Running roundtrip ==="
  cmd_roundtrip "$orig_conn" "GUI_XMDM%" "GUI_XMDM.XMDM_CONF%" "$compare_pattern" "$compare_data_pattern"

  echo ""
  echo "=== Roundtrip test complete ==="
}

run_sequala() {
  # Auto-detect mode if not explicitly set
  if [ "$SEQUALA_SBT_MODE" = "auto" ]; then
    if [ -n "$SEQUALA_CMD" ]; then
      # User provided custom command, use it as-is
      SEQUALA_SBT_MODE="false"
    elif [ -f "$SEQUALA_JAR" ]; then
      # JAR exists, use it
      SEQUALA_SBT_MODE="false"
    else
      # No JAR, fall back to sbt
      echo "Warning: JAR not found at $SEQUALA_JAR, building it now..."
      echo "Running: sbt cli/assembly"
      cd "$PROJECT_ROOT" && sbt cli/assembly >/dev/null 2>&1
      if [ -f "$SEQUALA_JAR" ]; then
        SEQUALA_SBT_MODE="false"
        echo "JAR built successfully"
      else
        echo "Warning: Failed to build JAR, falling back to sbt mode"
        SEQUALA_SBT_MODE="true"
      fi
    fi
  fi

  if [ "$SEQUALA_SBT_MODE" = "true" ]; then
    # For sbt, wrap all arguments in single quotes
    local cmd="${SEQUALA_CMD:-sbt}"
    $cmd "cli/run $*"
  else
    # For jar mode, use java with memory settings
    if [ -n "$SEQUALA_CMD" ]; then
      # User provided custom command
      # shellcheck disable=SC2086
      $SEQUALA_CMD "$@"
    else
      # Use JAR with memory settings
      if [ ! -f "$SEQUALA_JAR" ]; then
        echo "ERROR: JAR not found at $SEQUALA_JAR"
        echo "Please run: sbt cli/assembly"
        return 1
      fi
      # shellcheck disable=SC2086
      JAVA_OPTS="$SEQUALA_MEMORY" java $SEQUALA_MEMORY -jar "$SEQUALA_JAR" "$@"
    fi
  fi
}

run_migrations() {
  local sql_file="$1"
  local container="${2:-$DEFAULT_CONTAINER}"
  local conn="${3:-$DEFAULT_CONN_STRING}"

  if [ ! -f "$sql_file" ]; then
    echo "ERROR: SQL file not found: $sql_file"
    return 1
  fi

  local sqlplus_conn
  sqlplus_conn=$(get_sqlplus_conn "$conn")

  echo "=== Running migrations from $sql_file ==="
  echo "Container: $container"
  echo "Connection: $conn"
  echo ""

  # Create tablespaces that exist in production but not in Docker
  local tablespaces=("XMDM_FACT" "EUREX_FACT")
  for ts in "${tablespaces[@]}"; do
    echo "CREATE TABLESPACE $ts DATAFILE '$ts.dbf' SIZE 100M AUTOEXTEND ON;" | \
      docker exec -i "$container" sqlplus -s "$sqlplus_conn" >/dev/null 2>&1 || \
      echo "  Tablespace $ts already exists or could not be created"
  done

  # Count statements
  local total_statements
  total_statements=$(grep -c ';$' "$sql_file" || echo 0)
  echo "Loaded $total_statements statements from $sql_file"

  # Extract schemas from CREATE TABLE statements
  local schemas
  schemas=$(grep -i "^CREATE TABLE" "$sql_file" | \
    sed -n 's/.*CREATE TABLE[[:space:]]*"*\([A-Z_][A-Z0-9_]*\)"*\..*/\1/p' | \
    sort -u)

  if [ -n "$schemas" ]; then
    local schema_count
    schema_count=$(echo "$schemas" | wc -l | tr -d ' ')
    echo "Schemas found ($schema_count): $(echo "$schemas" | tr '\n' ' ')"
    echo ""

    # Create schemas (drop first if exists)
    local password
    password=$(parse_conn_string "$conn" password)
    while IFS= read -r schema; do
      [ -z "$schema" ] && continue
      echo "Creating schema $schema"
      {
        echo "DROP USER $schema CASCADE;" | \
          docker exec -i "$container" sqlplus -s "$sqlplus_conn" >/dev/null 2>&1 || true
        echo "CREATE USER $schema IDENTIFIED BY $password;"
        echo "GRANT CONNECT, RESOURCE TO $schema;"
        echo "ALTER USER $schema QUOTA UNLIMITED ON USERS;"
      } | docker exec -i "$container" sqlplus -s "$sqlplus_conn" >/dev/null 2>&1 || \
        echo "  Schema $schema already exists or could not be created"
    done <<< "$schemas"
  fi

  # Track statistics
  local success_count=0
  local fail_count=0
  local skipped_count=0
  local skipped_file="/tmp/migration-skipped-$$.txt"
  local failed_file="/tmp/migration-failed-$$.txt"
  > "$skipped_file"
  > "$failed_file"

  # Errors that indicate schema mismatches and should be logged
  local schema_mismatch_errors="ORA-00904|ORA-00947"

  # Execute entire SQL file at once - much faster than one-by-one!
  # Oracle will execute all statements and continue on errors
  echo "Executing all $total_statements statements in one batch..."
  echo "Oracle will continue executing even if some statements fail."
  echo ""

  local batch_output_file="/tmp/migration-batch-output-$$.txt"

  # Execute entire SQL file at once (this is fast - single docker exec call)
  time docker exec -i "$container" sqlplus -s "$sqlplus_conn" < "$sql_file" > "$batch_output_file" 2>&1

  echo ""
  echo "Batch execution completed. Analyzing results..."

  # Count errors from output
  local total_errors
  total_errors=$(grep -cE "^ORA-[0-9]+:" "$batch_output_file" 2>/dev/null | head -1 | tr -d '\n' || echo "0")
  total_errors=${total_errors:-0}

  echo "Found $total_errors error lines in output"
  echo ""
  echo "Now checking XMDM_CONF INSERT statements individually to identify schema mismatches..."

  # Check XMDM_CONF INSERT statements individually to log schema mismatches
  local idx=0
  while IFS= read -r statement || [ -n "$statement" ]; do
    # Remove trailing semicolon, preserve quotes and content
    statement=$(echo "$statement" | sed 's/;[[:space:]]*$//')
    # Trim leading/trailing whitespace without using xargs (which breaks on quotes)
    statement=$(echo "$statement" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
    [ -z "$statement" ] && continue

    idx=$((idx + 1))

    # Only check XMDM_CONF INSERT statements individually (these are likely to have schema mismatches)
    if echo "$statement" | grep -qiE "^INSERT INTO.*XMDM_CONF"; then
      if [ $((idx % 50)) -eq 0 ]; then
        echo "  Checking XMDM_CONF statements: $idx / $total_statements"
      fi

      local stmt_output
      stmt_output=$(echo "$statement;" | docker exec -i "$container" sqlplus -s "$sqlplus_conn" 2>&1)

      if echo "$stmt_output" | grep -qE "^ORA-[0-9]+:"; then
        local ora_error
        ora_error=$(echo "$stmt_output" | grep -oE "^ORA-[0-9]+:[^[:space:]]*" | head -1)

        case "$ora_error" in
          ORA-00955|ORA-01430|ORA-02260|ORA-02261|ORA-02264|ORA-02275|ORA-02443|ORA-01442|ORA-01451| \
          ORA-00942|ORA-01917|ORA-01918|ORA-01418|ORA-04043|ORA-02019| \
          ORA-00904|ORA-00947|ORA-12899|ORA-00001| \
          ORA-00957|ORA-01789| \
          ORA-04063|ORA-00990|ORA-38824|ORA-00900|ORA-01735|ORA-06550|ORA-03405| \
          ORA-01740|ORA-03048|ORA-01742|ORA-02185|ORA-01756)
            skipped_count=$((skipped_count + 1))
            if echo "$ora_error" | grep -qE "$schema_mismatch_errors"; then
              {
                echo "=== Statement $idx ==="
                echo "$statement" | head -c 200
                echo "..."
                echo "Error: $ora_error"
                echo ""
              } >> "$skipped_file"
            fi
            ;;
          *)
            fail_count=$((fail_count + 1))
            {
              echo "=== Statement $idx ==="
              echo "$statement" | head -c 200
              echo "..."
              echo "Error: $ora_error"
              echo "$stmt_output" | head -5
              echo ""
            } >> "$failed_file"
            ;;
        esac
      else
        success_count=$((success_count + 1))
      fi
    fi
  done < "$sql_file"

  # Count successful statements
  # Oracle executes all statements and continues on errors
  # Most statements succeeded, but we need to account for the ones we checked individually
  local xmdm_conf_checked
  xmdm_conf_checked=$(grep -cE "^INSERT INTO.*XMDM_CONF" "$sql_file" || echo "0")
  # For XMDM_CONF statements we checked individually, use actual results
  # For others, estimate based on total errors
  local other_statements=$((total_statements - xmdm_conf_checked))
  local other_errors=$((total_errors - (skipped_count + fail_count)))
  local other_success=$((other_statements - other_errors))
  success_count=$((success_count + other_success))

  # Cleanup
  rm -f "$batch_output_file"

  # Commit
  echo "COMMIT;" | docker exec -i "$container" sqlplus -s "$sqlplus_conn" >/dev/null 2>&1 || true

  # Print summary
  echo ""
  echo "=== SUMMARY ==="
  echo "Successful: $success_count"
  echo "Skipped (ignorable errors): $skipped_count"
  echo "Failed: $fail_count"

  # Print skipped schema mismatches
  if [ -s "$skipped_file" ]; then
    local skipped_mismatch_count
    skipped_mismatch_count=$(grep -c "=== Statement" "$skipped_file" || echo 0)
    echo ""
    echo "=== SKIPPED STATEMENTS (Schema Mismatches) ==="
    echo "Total skipped due to schema mismatches: $skipped_mismatch_count"
    echo ""
    echo "=== SAMPLE SKIPPED STATEMENTS (first 20) ==="
    head -80 "$skipped_file"
  fi

  # Print failures
  if [ -s "$failed_file" ]; then
    local failed_error_count
    failed_error_count=$(grep -c "=== Statement" "$failed_file" || echo 0)
    echo ""
    echo "=== ERROR SUMMARY ==="
    echo "Total failures: $failed_error_count"
    echo ""
    echo "=== SAMPLE FAILURES (first 10) ==="
    head -50 "$failed_file"
  fi

  # Cleanup
  rm -f "$skipped_file" "$failed_file"

  echo ""
  echo "Done."
}

cmd_migrate() {
  local sql_file="$1"
  local container="${2:-$DEFAULT_CONTAINER}"
  local conn="${3:-$DEFAULT_CONN_STRING}"

  if [ -z "$sql_file" ]; then
    echo "Usage: $0 migrate <sql-file> [container] [connection]"
    echo ""
    echo "Run SQL migrations against an Oracle instance via docker exec."
    echo ""
    echo "Arguments:"
    echo "  sql-file    SQL file containing migrations to run (required)"
    echo "  container   Docker container name (default: $DEFAULT_CONTAINER)"
    echo "  connection  Connection string: user/pass@//host:port/service (default: $DEFAULT_CONN_STRING)"
    echo ""
    echo "Environment variables:"
    echo "  DEFAULT_CONTAINER    Default container (current: $DEFAULT_CONTAINER)"
    echo "  DEFAULT_CONN_STRING  Default connection string"
    echo ""
    echo "Examples:"
    echo "  $0 migrate migrations.sql"
    echo "  $0 migrate migrations.sql oracle-new"
    echo "  $0 migrate migrations.sql oracle-new system/pass@//localhost:1521/ORCL"
    exit 1
  fi

  if [ ! -f "$sql_file" ]; then
    echo "ERROR: SQL file not found: $sql_file"
    exit 1
  fi

  run_migrations "$sql_file" "$container" "$conn"
}

cmd_migrate_orig() {
  local conn="${1:-$DEFAULT_CONN_STRING}"
  local output_file="$PROJECT_ROOT/sorted-migrations.sql"

  echo "=== Generating sorted migrations ==="
  cd "$PROJECT_ROOT"

  # Find all SQL files excluding DESIRED_STATE directory
  local sql_files
  sql_files=$(find "$SCRIPT_DIR" -name "*.sql" -type f ! -path "*/DESIRED_STATE/*" | sort)

  if [ -z "$sql_files" ]; then
    echo "ERROR: No SQL files found"
    exit 1
  fi

  local file_count
  file_count=$(echo "$sql_files" | wc -l | tr -d ' ')
  echo "Found $file_count SQL files (excluding DESIRED_STATE)"

  # Run sequala to parse, simplify (filter noise, dedupe, sort), and output SQL
  # shellcheck disable=SC2086
  run_sequala parse oracle \
    --simplify \
    --output sql \
    --write-to "$output_file" \
    $sql_files

  if [ ! -f "$output_file" ]; then
    echo "ERROR: Failed to generate $output_file"
    exit 1
  fi

  echo "=== Generated $output_file ==="
  echo "Statement count: $(grep -c ';$' "$output_file" || echo 0)"
  echo ""

  echo "=== Running migrations against $ORIG_CONTAINER ==="

  # Run migrations using Bash function
  cd "$PROJECT_ROOT"
  run_migrations "$output_file" "$ORIG_CONTAINER" "$conn"
}

cmd_migrate_new() {
  # Default uses port 1522 for oracle-new container, same service as DEFAULT_CONN_STRING
  local service
  service=$(parse_conn_string "$DEFAULT_CONN_STRING" service)
  local conn="${1:-system/$ORACLE_PASSWORD@//localhost:1522/$service}"
  local combined_file="$PROJECT_ROOT/new-schema-migrations.sql"

  echo "=== Finding *-PROD-tables.sql files ==="
  cd "$PROJECT_ROOT"

  local prod_files
  prod_files=$(find "$SCRIPT_DIR/DESIRED_STATE" -name "*-PROD-tables.sql" -type f 2>/dev/null)

  if [ -z "$prod_files" ]; then
    echo "ERROR: No *-PROD-tables.sql files found in $SCRIPT_DIR/DESIRED_STATE/"
    exit 1
  fi

  echo "Found files:"
  echo "$prod_files" | sed 's/^/  /'
  echo ""

  # Clear combined output file
  > "$combined_file"

  for prod_file in $prod_files; do
    echo "=== Processing $prod_file ==="
    local base_name
    base_name=$(basename "$prod_file" .sql)
    local ddl_output="$PROJECT_ROOT/${base_name}-ddl.sql"
    local audit_output="$PROJECT_ROOT/${base_name}-audit.sql"
    local metadata_output="$PROJECT_ROOT/${base_name}-metadata.sql"

    # Generate base DDL (pass through as-is using identity jq to re-emit SQL)
    echo "  Generating base DDL..."
    run_sequala parse oracle \
      --output sql \
      --write-to "$ddl_output" \
      "$prod_file"

    # Generate audit/history table DDL (_AT and _HT tables)
    echo "  Generating derived tables..."
    run_sequala parse oracle \
      --output "jq-file-sql:$SCRIPT_DIR/ddl-to-derived-tables.jq" \
      --write-to "$audit_output" \
      "$prod_file"

    # Generate metadata INSERTs
    echo "  Generating metadata..."
    run_sequala parse oracle \
      --output "jq-file-sql:$SCRIPT_DIR/ddl-to-metadata.jq" \
      --write-to "$metadata_output" \
      "$prod_file"

    # Append all to combined file (DDL first, then audit, then metadata)
    echo "-- From: $prod_file (DDL)" >> "$combined_file"
    cat "$ddl_output" >> "$combined_file"
    echo "" >> "$combined_file"

    echo "-- From: $prod_file (Audit Tables)" >> "$combined_file"
    cat "$audit_output" >> "$combined_file"
    echo "" >> "$combined_file"

    echo "-- From: $prod_file (Metadata)" >> "$combined_file"
    cat "$metadata_output" >> "$combined_file"
    echo "" >> "$combined_file"

    # Clean up intermediate files
    rm -f "$ddl_output" "$audit_output" "$metadata_output"
  done

  echo "=== Generated $combined_file ==="
  echo "Statement count: $(grep -c ';$' "$combined_file" || echo 0)"
  echo ""

  echo "=== Running migrations against $NEW_CONTAINER ==="

  cd "$PROJECT_ROOT"
  run_migrations "$combined_file" "$NEW_CONTAINER" "$conn"
}

cmd_metadata_to_ddl() {
  local input_file="$1"
  local output_file="$2"

  if [ -z "$input_file" ]; then
    echo "Usage: $0 metadata-to-ddl <configure-file.sql> [output-file.sql]"
    echo ""
    echo "Converts XMDM_CONF_TABLE/COLUMN INSERTs to DDL with embedded COMMENT metadata."
    echo "This is the reverse of ddl-to-metadata."
    echo ""
    echo "Examples:"
    echo "  $0 metadata-to-ddl PMDS/02_CONFIGURE_XMDM_CONF_TABLES.sql"
    echo "  $0 metadata-to-ddl PMDS/02_CONFIGURE_XMDM_CONF_TABLES.sql PMDS-tables.sql"
    exit 1
  fi

  if [ ! -f "$input_file" ]; then
    echo "ERROR: Input file not found: $input_file"
    exit 1
  fi

  cd "$PROJECT_ROOT"

  if [ -n "$output_file" ]; then
    echo "=== Converting metadata to DDL ==="
    echo "Input:  $input_file"
    echo "Output: $output_file"
    run_sequala parse oracle \
      --output "jq-file-sql:$SCRIPT_DIR/metadata-to-ddl.jq" \
      --pretty true \
      --write-to "$output_file" \
      "$input_file"
    echo ""
    if [[ -d "$output_file" ]]; then
      echo "Created directories:"
      ls -1 "$output_file"
    else
      echo "Statement count: $(grep -c ';$' "$output_file" || echo 0)"
    fi
  else
    echo "=== Converting metadata to DDL ===" >&2
    echo "Input: $input_file" >&2
    echo "" >&2
    run_sequala parse oracle \
      --output "jq-file-sql:$SCRIPT_DIR/metadata-to-ddl.jq" \
      "$input_file"
  fi
}

cmd_generate_prod_tables() {
  local force="${1:-}"

  echo "=== Generating PROD table files from all schema directories ==="
  cd "$PROJECT_ROOT"

  local output_dir="$SCRIPT_DIR/DESIRED_STATE"

  # Ensure DESIRED_STATE directory exists
  mkdir -p "$output_dir"

  # Check if we should skip (only if not forcing and files exist)
  if [ "$force" != "--force" ]; then
    local existing_files
    existing_files=$(ls "$output_dir"/*-PROD-tables.sql 2>/dev/null | wc -l)
    if [ "$existing_files" -gt 0 ]; then
      echo "  Files already exist in $output_dir"
      echo "  Use --force to regenerate"
      echo ""
      echo "Existing files:"
      ls -la "$output_dir"/*-PROD-tables.* 2>/dev/null
      return 0
    fi
  fi

  # Find all schema directories (excluding DESIRED_STATE and INITIAL)
  local schema_dirs
  schema_dirs=$(find "$SCRIPT_DIR" -mindepth 1 -maxdepth 1 -type d \
    ! -name "DESIRED_STATE" \
    ! -name "INITIAL" \
    ! -name ".*" \
    2>/dev/null | sort)

  if [ -z "$schema_dirs" ]; then
    echo "ERROR: No schema directories found"
    exit 1
  fi

  local dir_count
  dir_count=$(echo "$schema_dirs" | wc -l | tr -d ' ')
  echo "  Found $dir_count schema directories"
  echo "  Output directory: $output_dir"
  echo ""

  local success_count=0
  local fail_count=0

  for schema_dir in $schema_dirs; do
    local schema_name
    schema_name=$(basename "$schema_dir")
    local output_file="$output_dir/${schema_name}-PROD-tables.sql"

    # Find SQL files in this schema directory
    local input_files
    input_files=$(find "$schema_dir" -name "*.sql" -type f 2>/dev/null | sort | tr '\n' ' ')

    if [ -z "$input_files" ]; then
      echo "  Skipping $schema_name (no SQL files)"
      continue
    fi

    local file_count
    file_count=$(echo "$input_files" | wc -w | tr -d ' ')
    echo "  Processing $schema_name ($file_count files)..."

    # Run sequala with all files - batch processing with @write-map
    # shellcheck disable=SC2086
    if run_sequala parse oracle \
        --output "jq-file-sql:$SCRIPT_DIR/merge-ddl-and-metadata.jq" \
        --pretty true \
        --write-to "$output_dir/" \
        $input_files 2>&1 | tee "$output_dir/${schema_name}.log" | grep -q "^\[success\]"; then
      if [ -f "$output_file" ]; then
        success_count=$((success_count + 1))
        echo "    Created $output_file"
      else
        echo "    No tables with metadata in $schema_name"
      fi
    else
      echo "    ERROR: Failed to process $schema_name"
      fail_count=$((fail_count + 1))
    fi
  done

  echo ""
  echo "=== Summary ==="
  echo "  Successful: $success_count"
  echo "  Failed: $fail_count"
  echo ""
  echo "=== Generated files ==="
  ls -la "$output_dir"/*-PROD-tables.sql 2>/dev/null || echo "  (none)"
}

cmd_compare_desired_state() {
  local output_dir="${1:-$PROJECT_ROOT/migrations}"
  local conn="${2:-$DEFAULT_CONN_STRING}"
  local schema_pattern="${3:-GUI_XMDM%}"

  mkdir -p "$output_dir"

  echo "=== Comparing DESIRED_STATE against database ==="
  echo "  Database: $conn"
  echo "  Schema:   $schema_pattern"
  echo "  Output:   $output_dir/"
  echo "  (Generating _AT and _HT tables on-the-fly via --source-transform)"
  echo ""

  # Single sequala call handles:
  # - Glob pattern for multiple source files
  # - Schema pattern to filter/expand schemas (SQL LIKE: % any, _ single char)
  # - Source transformation to generate derived _AT/_HT tables
  # - Per-schema output files
  run_sequala plan \
    --source "$SCRIPT_DIR/DESIRED_STATE/*-PROD-tables.sql" \
    --database "$(to_jdbc_url "$conn")" \
    --schema "$schema_pattern" \
    --dialect oracle \
    --format sql \
    --pretty true \
    --source-transform "jq:$SCRIPT_DIR/ddl-to-derived-tables.jq" \
    --transform "exclude:DropTable" \
    --write-to "$output_dir/"
}

cmd_sync() {
  local output_dir="${1:-$SCRIPT_DIR/MIGRATIONS}"
  local conn="${2:-$DEFAULT_CONN_STRING}"
  local schema_pattern="${3:-GUI_XMDM%}"

  mkdir -p "$output_dir"

  echo "=== Sync: Combined schema and data migration ==="
  echo "  Database: $conn"
  echo "  Schema:   $schema_pattern"
  echo "  Output:   $output_dir/"
  echo "  (DDL from DESIRED_STATE, generating _AT/_HT tables)"
  echo "  (Data sync for XMDM_CONF tables with INSERT/UPDATE/DELETE via --ddls-from @database)"
  echo ""

  # Unified sync command handles both schema and data:
  # - Parses DDL with embedded metadata comments from desired state files
  # - Applies source transform (ddl-to-sync.jq) to generate:
  #   1. Derived _AT/_HT tables (for schema diffing)
  #   2. INSERT statements for XMDM_CONF_TABLE and XMDM_CONF_COLUMN
  # - Uses --ddls-from @database to get PK info from existing tables
  #   (enables proper UPDATE/DELETE generation instead of just INSERTs)
  # - Compares schema against database
  # - Outputs DDL migrations per schema
  run_sequala sync \
    --desired "$SCRIPT_DIR/DESIRED_STATE/*-PROD-tables.sql" \
    --database "$(to_jdbc_url "$conn")" \
    --schema "$schema_pattern" \
    --dialect oracle \
    --format sql \
    --pretty true \
    --source-transform "jq:$SCRIPT_DIR/ddl-to-sync.jq" \
    --ddls-from @database \
    --with-deletes false \
    --write-to "$output_dir/"
}

cmd_compare() {
  local pattern="${1:-GUI_XMDM%}"
  local data_pattern="${2:-GUI_XMDM.XMDM_CONF%}"
  local container="${3:-$NEW_CONTAINER}"
  local conn="${4:-$DEFAULT_CONN_STRING}"
  local sqlplus_conn
  sqlplus_conn=$(get_sqlplus_conn "$conn")

  echo "=== Comparing tables matching '$pattern' ==="
  echo "  Container: $container"
  echo "  Local vs Remote (via orig_db link)"
  echo "  Data comparison: $data_pattern"
  echo ""

  docker exec "$container" sqlplus -s "$sqlplus_conn" \
    @/scripts/compare.sql "$pattern" "$data_pattern"
}

cmd_dump_unified() {
  local conn="${1:-}"
  local ddls_pattern="${2:-%}"
  local data_pattern="${3:-%.XMDM_CONF%}"
  local output_dir="${4:-$SCRIPT_DIR/DESIRED_STATE}"
  local unified_filter="${5:-$SCRIPT_DIR/unified-dump-filter.jq}"

  if [ -z "$conn" ]; then
    echo "Usage: $0 dump-unified <connection> [ddls-pattern] [data-pattern] [output-dir] [unified-filter]"
    echo ""
    echo "Exports DDL and data from the database using a unified filter."
    echo ""
    echo "Arguments:"
    echo "  connection     Connection string: user/pass@//host:port/service (required)"
    echo "                 Example: system/pass@//localhost:1521/FREEPDB1"
    echo "  ddls-pattern   Schema or schema.table pattern for DDL export (SQL LIKE syntax)"
    echo "                 Default: % (all schemas)"
    echo "                 Examples: GUI_XMDM% (all matching schemas), GUI_XMDM_F7.MAP% (specific tables)"
    echo "  data-pattern   Schema.table pattern for data export"
    echo "                 Default: %.XMDM_CONF% (XMDM_CONF tables in all schemas)"
    echo "                 Examples: GUI_XMDM.XMDM_CONF%, GUI_XMDM_F7.%"
    echo "  output-dir     Output directory (default: DESIRED_STATE relative to script)"
    echo "  unified-filter JQ filter file for combined DDL and data transformation"
    echo "                 Default: unified-dump-filter.jq"
    echo ""
    echo "Output: One SQL file and one YAML file per schema named {SCHEMA}-PROD-tables.{sql,yaml}"
    echo ""
    echo "Examples:"
    echo "  $0 dump-unified system/pass@//localhost:1521/FREEPDB1"
    echo "  $0 dump-unified system/pass@//localhost:1521/FREEPDB1 GUI_XMDM%"
    echo "  $0 dump-unified system/pass@//localhost:1521/FREEPDB1 GUI_XMDM% GUI_XMDM.XMDM_CONF%"
    exit 1
  fi

  echo "=== Dumping database (unified filter) ==="
  echo "  Database: $conn"
  echo "  DDL pattern: $ddls_pattern"
  echo "  Data pattern: $data_pattern"
  echo "  Unified filter: $unified_filter"
  echo "  Output: $output_dir"
  echo ""

  mkdir -p "$output_dir"
  cd "$PROJECT_ROOT"

  local args=""
  args="$args --database $(to_jdbc_url "$conn")"
  args="$args --outputDir $output_dir"
  args="$args --pretty true"
  args="$args --ddls $ddls_pattern"
  args="$args --data $data_pattern"
  args="$args --filter $unified_filter"

  # shellcheck disable=SC2086
  run_sequala dump $args
}

cmd_test_sync() {
  local test_schema="GUI_XMDM_STX_TESTS"
  local test_file="$SCRIPT_DIR/DESIRED_STATE/${test_schema}-PROD-tables.sql"
  local test_file_backup="$SCRIPT_DIR/DESIRED_STATE/${test_schema}-PROD-tables.sql.bak"
  local container="$NEW_CONTAINER"
  local service
  service=$(parse_conn_string "$DEFAULT_CONN_STRING" service)
  local conn="system/$ORACLE_PASSWORD@//localhost:1522/$service"
  local output_dir="$SCRIPT_DIR/TEST_MIGRATIONS"

  echo "=== Test: Sync with --ddls-from @database ==="
  echo "  Container: $container"
  echo "  Test schema: $test_schema"
  echo "  Test file: $test_file"
  echo ""

  # Cleanup from previous runs
  rm -rf "$output_dir"
  mkdir -p "$output_dir"

  # Step 1: Ensure container is running
  echo "=== Step 1: Checking container status ==="
  if ! docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
    echo "Container $container not running. Starting it..."
    cmd_start_single "$container" 1522
  else
    echo "Container $container is already running"
  fi

  # Step 2: Create GUI_XMDM schema with XMDM_CONF tables
  echo ""
  echo "=== Step 2: Creating GUI_XMDM schema with XMDM_CONF tables ==="
  local sqlplus_conn
  sqlplus_conn=$(get_sqlplus_conn "$conn")

  # Create GUI_XMDM user/schema
  {
    echo "DROP USER GUI_XMDM CASCADE;" | docker exec -i "$container" sqlplus -s "$sqlplus_conn" 2>/dev/null || true
    echo "CREATE USER GUI_XMDM IDENTIFIED BY $ORACLE_PASSWORD;"
    echo "GRANT CONNECT, RESOURCE TO GUI_XMDM;"
    echo "ALTER USER GUI_XMDM QUOTA UNLIMITED ON USERS;"
  } | docker exec -i "$container" sqlplus -s "$sqlplus_conn" >/dev/null 2>&1
  echo "  Created GUI_XMDM schema"

  # Create XMDM_CONF_TABLE
  docker exec -i "$container" sqlplus -s "$sqlplus_conn" <<'EOSQL' >/dev/null 2>&1
CREATE TABLE "GUI_XMDM"."XMDM_CONF_TABLE" (
  "APP_NAME" VARCHAR2(20 BYTE) NOT NULL,
  "TBL_NAME" VARCHAR2(61 BYTE) NOT NULL,
  "TBL_GUI_NAME" VARCHAR2(150 BYTE),
  "TBL_DESC" VARCHAR2(250 BYTE),
  "TBL_DISPLAY_ORDER" NUMBER(3, 0),
  "TBL_GUI_NAME_SHORT" VARCHAR2(31 BYTE),
  "TBL_SCHEMA" VARCHAR2(50 BYTE),
  "TBL_NAME_AT" VARCHAR2(64 BYTE),
  "HISTORICAL_TBL_FLAG" CHAR(1 BYTE),
  PRIMARY KEY ("APP_NAME", "TBL_NAME")
);
EOSQL
  echo "  Created XMDM_CONF_TABLE"

  # Create XMDM_CONF_COLUMN
  docker exec -i "$container" sqlplus -s "$sqlplus_conn" <<'EOSQL' >/dev/null 2>&1
CREATE TABLE "GUI_XMDM"."XMDM_CONF_COLUMN" (
  "APP_NAME" VARCHAR2(20 BYTE) NOT NULL,
  "TBL_NAME" VARCHAR2(61 BYTE) NOT NULL,
  "COL_NAME" VARCHAR2(61 BYTE) NOT NULL,
  "COL_GUI_NAME" VARCHAR2(150 BYTE),
  "COL_DESC" VARCHAR2(250 BYTE),
  "COL_FORMAT" VARCHAR2(80 BYTE),
  "COL_DISPLAY_ORDER" NUMBER(3, 0),
  "COL_PK" CHAR(1 BYTE),
  "COL_REQUIRED" CHAR(1 BYTE),
  "COL_EDITABLE" CHAR(1 BYTE),
  "COL_DATATYPE" CHAR(1 BYTE),
  "COL_VALIDATOR" VARCHAR2(200 BYTE),
  "COL_MULTILINE" CHAR(1 BYTE),
  "COL_VALID_INTERVAL_COLUMN" VARCHAR2(61 BYTE),
  "COL_AUTOINCREMENT" CHAR(1 BYTE),
  "COL_DEFAULT_VAL" VARCHAR2(61 BYTE),
  PRIMARY KEY ("APP_NAME", "TBL_NAME", "COL_NAME")
);
EOSQL
  echo "  Created XMDM_CONF_COLUMN"

  # Step 3: Create test schema
  echo ""
  echo "=== Step 3: Creating test schema $test_schema ==="
  {
    echo "DROP USER $test_schema CASCADE;" | docker exec -i "$container" sqlplus -s "$sqlplus_conn" 2>/dev/null || true
    echo "CREATE USER $test_schema IDENTIFIED BY $ORACLE_PASSWORD;"
    echo "GRANT CONNECT, RESOURCE TO $test_schema;"
    echo "ALTER USER $test_schema QUOTA UNLIMITED ON USERS;"
  } | docker exec -i "$container" sqlplus -s "$sqlplus_conn" >/dev/null 2>&1
  echo "  Created $test_schema schema"

  # Create test table (strip metadata comments for DDL)
  grep -v "^-- @" "$test_file" | sed 's/TABLESPACE XMDM_FACT//' | \
    docker exec -i "$container" sqlplus -s "$sqlplus_conn" >/dev/null 2>&1
  echo "  Created test table S_TEST_MMPM"

  # Step 4: Run initial sync to populate XMDM_CONF tables
  # Use GUI_XMDM% schema pattern to include both GUI_XMDM (for XMDM_CONF tables PK info)
  # and GUI_XMDM_STX_TESTS (for the test schema)
  echo ""
  echo "=== Step 4: Running initial sync ==="
  run_sequala sync \
    --desired "$test_file" \
    --database "$(to_jdbc_url "$conn")" \
    --schema "GUI_XMDM%" \
    --dialect oracle \
    --format sql \
    --pretty true \
    --source-transform "jq:$SCRIPT_DIR/ddl-to-sync.jq" \
    --ddls-from @database \
    --with-deletes false \
    --write-to "$output_dir/" 2>&1 | tee "$output_dir/initial-sync.log"

  # Apply the generated migrations (metadata INSERTs go to GUI_XMDM-sync.sql)
  echo ""
  echo "=== Step 5: Applying initial migrations ==="
  local gui_xmdm_sync="$output_dir/GUI_XMDM-sync.sql"
  if [ -f "$gui_xmdm_sync" ]; then
    echo "  Applying $gui_xmdm_sync (contains XMDM_CONF INSERTs)"
    # Fix double semicolons that sequala generates
    sed 's/;;$/;/' "$gui_xmdm_sync" | docker exec -i "$container" sqlplus -s "$sqlplus_conn"
    echo "COMMIT;" | docker exec -i "$container" sqlplus -s "$sqlplus_conn"
  else
    echo "  No GUI_XMDM-sync.sql generated"
  fi

  # Query initial state
  echo ""
  echo "=== Step 6: Querying initial metadata state ==="
  echo "SELECT APP_NAME, TBL_NAME, TBL_GUI_NAME FROM GUI_XMDM.XMDM_CONF_TABLE WHERE APP_NAME = '$test_schema';" | \
    docker exec -i "$container" sqlplus -s "$sqlplus_conn"

  local initial_gui_name
  initial_gui_name=$(echo "SELECT TBL_GUI_NAME FROM GUI_XMDM.XMDM_CONF_TABLE WHERE APP_NAME = '$test_schema' AND TBL_NAME = 'S_TEST_MMPM';" | \
    docker exec -i "$container" sqlplus -s "$sqlplus_conn" | grep -v "^$" | grep -v "TBL_GUI_NAME" | grep -v "^-" | head -1 | xargs)
  echo "  Initial TBL_GUI_NAME: '$initial_gui_name'"

  # Step 7: Modify the test file
  echo ""
  echo "=== Step 7: Modifying test file (changing TBL_GUI_NAME) ==="
  cp "$test_file" "$test_file_backup"
  # Change "MMPM tests" to "MMPM Tests MODIFIED"
  sed -i.tmp 's/@TBL_GUI_NAME: MMPM tests/@TBL_GUI_NAME: MMPM Tests MODIFIED/' "$test_file"
  rm -f "$test_file.tmp"
  echo "  Changed @TBL_GUI_NAME from 'MMPM tests' to 'MMPM Tests MODIFIED'"
  grep "@TBL_GUI_NAME" "$test_file"

  # Step 8: Run sync again
  echo ""
  echo "=== Step 8: Running sync after modification ==="
  rm -f "$output_dir"/*.sql "$output_dir"/*.log 2>/dev/null || true
  run_sequala sync \
    --desired "$test_file" \
    --database "$(to_jdbc_url "$conn")" \
    --schema "GUI_XMDM%" \
    --dialect oracle \
    --format sql \
    --pretty true \
    --source-transform "jq:$SCRIPT_DIR/ddl-to-sync.jq" \
    --ddls-from @database \
    --with-deletes false \
    --write-to "$output_dir/" 2>&1 | tee "$output_dir/modified-sync.log"

  # Check what was generated
  echo ""
  echo "=== Step 9: Checking generated migration ==="
  if [ -f "$gui_xmdm_sync" ]; then
    echo "  Generated migration file:"
    cat "$gui_xmdm_sync"

    # Check for UPDATE vs INSERT
    if grep -q "UPDATE" "$gui_xmdm_sync"; then
      echo ""
      echo "  ✓ SUCCESS: UPDATE statement generated (not duplicate INSERT)"
    elif grep -q "INSERT" "$gui_xmdm_sync"; then
      echo ""
      echo "  ✗ FAILURE: INSERT statement generated (would create duplicates)"
    else
      echo ""
      echo "  ? No INSERT or UPDATE found in migration file"
    fi
  else
    echo "  No GUI_XMDM-sync.sql generated"
  fi

  # Apply the migration
  echo ""
  echo "=== Step 10: Applying modified migration ==="
  if [ -f "$gui_xmdm_sync" ]; then
    sed 's/;;$/;/' "$gui_xmdm_sync" | docker exec -i "$container" sqlplus -s "$sqlplus_conn"
    echo "COMMIT;" | docker exec -i "$container" sqlplus -s "$sqlplus_conn"
  fi

  # Verify the change
  echo ""
  echo "=== Step 11: Verifying the change ==="
  echo "SELECT APP_NAME, TBL_NAME, TBL_GUI_NAME FROM GUI_XMDM.XMDM_CONF_TABLE WHERE APP_NAME = '$test_schema';" | \
    docker exec -i "$container" sqlplus -s "$sqlplus_conn"

  local final_gui_name
  final_gui_name=$(echo "SELECT TBL_GUI_NAME FROM GUI_XMDM.XMDM_CONF_TABLE WHERE APP_NAME = '$test_schema' AND TBL_NAME = 'S_TEST_MMPM';" | \
    docker exec -i "$container" sqlplus -s "$sqlplus_conn" | grep -v "^$" | grep -v "TBL_GUI_NAME" | grep -v "^-" | head -1 | xargs)
  echo "  Final TBL_GUI_NAME: '$final_gui_name'"

  # Check row count (should be 1, not 2)
  local row_count
  row_count=$(echo "SELECT COUNT(*) FROM GUI_XMDM.XMDM_CONF_TABLE WHERE APP_NAME = '$test_schema' AND TBL_NAME = 'S_TEST_MMPM';" | \
    docker exec -i "$container" sqlplus -s "$sqlplus_conn" | grep -E "^\s*[0-9]+\s*$" | xargs)
  echo "  Row count: $row_count"

  # Restore original file
  echo ""
  echo "=== Step 12: Restoring original test file ==="
  mv "$test_file_backup" "$test_file"
  echo "  Restored $test_file"

  # Final verdict
  echo ""
  echo "=== TEST RESULTS ==="
  if [ "$row_count" = "1" ] && [ "$final_gui_name" = "MMPM Tests MODIFIED" ]; then
    echo "  ✓ SUCCESS: Row was UPDATED (count=1, value changed)"
    echo "  --ddls-from @database is working correctly!"
  else
    echo "  ✗ FAILURE: Expected count=1 and value='MMPM Tests MODIFIED'"
    echo "    Actual count=$row_count, value='$final_gui_name'"
  fi
}

cmd_roundtrip() {
  local source_conn="${1:-}"
  local ddls_pattern="${2:-%}"
  local data_pattern="${3:-%.XMDM_CONF%}"
  local compare_pattern="${4:-GUI_XMDM%}"
  local compare_data_pattern="${5:-GUI_XMDM.XMDM_CONF%}"

  if [ -z "$source_conn" ]; then
    echo "Usage: $0 roundtrip <source-conn> [ddls-pattern] [data-pattern] [compare-pattern] [compare-data-pattern]"
    echo ""
    echo "End-to-end roundtrip test: dump from source DB, apply to oracle-new, compare."
    echo "This tests whether information is preserved through the dump/migrate cycle."
    echo ""
    echo "Arguments:"
    echo "  source-conn          Source connection: user/pass@//host:port/service (required)"
    echo "  ddls-pattern         Schema pattern for DDL export (default: %)"
    echo "  data-pattern         Schema.table pattern for data export (default: %.XMDM_CONF%)"
    echo "  compare-pattern      Schema pattern for structure comparison (default: GUI_XMDM%)"
    echo "  compare-data-pattern Schema.table pattern for data comparison (default: GUI_XMDM.XMDM_CONF%)"
    echo ""
    echo "Steps performed:"
    echo "  1. Dump schema and metadata from source DB to DESIRED_STATE"
    echo "  2. Start oracle-new container (if not running)"
    echo "  3. Apply migrations to oracle-new"
    echo "  4. Create database link from oracle-new to source DB"
    echo "  5. Compare oracle-new against source DB"
    echo ""
    echo "Examples:"
    echo "  $0 roundtrip system/pass@//prod:1521/ORCL"
    echo "  $0 roundtrip system/pass@//prod:1521/ORCL GUI_XMDM%"
    echo "  $0 roundtrip system/pass@//prod:1521/ORCL GUI_XMDM% GUI_XMDM.XMDM_CONF% GUI_XMDM% GUI_XMDM.XMDM_CONF%"
    exit 1
  fi

  local output_dir="$SCRIPT_DIR/ROUNDTRIP_STATE"
  local migrations_file="$output_dir/roundtrip-migrations.sql"
  local container="$NEW_CONTAINER"
  local service
  service=$(parse_conn_string "$DEFAULT_CONN_STRING" service)
  local new_conn="system/$ORACLE_PASSWORD@//localhost:1522/$service"
  local sqlplus_conn
  sqlplus_conn=$(get_sqlplus_conn "$new_conn")

  echo "=== Roundtrip Test ==="
  echo "  Source DB: $source_conn"
  echo "  DDL pattern: $ddls_pattern"
  echo "  Data pattern: $data_pattern"
  echo "  Compare pattern: $compare_pattern"
  echo "  Compare data pattern: $compare_data_pattern"
  echo "  Output: $output_dir"
  echo ""

  # Cleanup from previous runs
  rm -rf "$output_dir"
  mkdir -p "$output_dir"

  # Step 1: Dump schema and metadata from source DB
  echo "=== Step 1: Dumping schema and metadata from source DB ==="
  cmd_dump_unified "$source_conn" "$ddls_pattern" "$data_pattern" "$output_dir"

  # Find generated files and combine into single migration file
  echo ""
  echo "=== Step 1b: Combining generated files ==="
  local prod_files
  prod_files=$(find "$output_dir" -name "*-PROD-tables.sql" -type f 2>/dev/null | sort)

  if [ -z "$prod_files" ]; then
    echo "ERROR: No *-PROD-tables.sql files generated"
    exit 1
  fi

  echo "Found files:"
  echo "$prod_files" | sed 's/^/  /'

  # Combine all files
  > "$migrations_file"
  for prod_file in $prod_files; do
    echo "-- From: $prod_file" >> "$migrations_file"
    cat "$prod_file" >> "$migrations_file"
    echo "" >> "$migrations_file"
  done

  echo "Combined into: $migrations_file"
  echo "Statement count: $(grep -c ';$' "$migrations_file" || echo 0)"

  # Step 2: Ensure oracle-new container is running
  echo ""
  echo "=== Step 2: Ensuring oracle-new container is running ==="
  if ! docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
    echo "Container $container not running. Starting it..."
    cmd_start_single "$container" 1522
  else
    echo "Container $container is already running"
  fi

  # Step 3: Apply migrations to oracle-new
  echo ""
  echo "=== Step 3: Applying migrations to oracle-new ==="
  run_migrations "$migrations_file" "$container" "$new_conn"

  # Step 4: Create database link from oracle-new to source DB
  echo ""
  echo "=== Step 4: Creating database link to source DB ==="

  local source_user source_password source_host source_port source_service
  source_user=$(parse_conn_string "$source_conn" user)
  source_password=$(parse_conn_string "$source_conn" password)
  source_host=$(parse_conn_string "$source_conn" host)
  source_port=$(parse_conn_string "$source_conn" port)
  source_service=$(parse_conn_string "$source_conn" service)

  # Drop existing link if any
  echo "DROP DATABASE LINK orig_db;" | docker exec -i "$container" sqlplus -s "$sqlplus_conn" 2>/dev/null || true

  # Create link (named orig_db to match compare.sql expectations)
  run_sql "$container" \
    "CREATE DATABASE LINK orig_db CONNECT TO $source_user IDENTIFIED BY $source_password USING '$source_host:$source_port/$source_service';" \
    "$new_conn"

  echo "Testing link..."
  run_sql "$container" "SELECT * FROM dual@orig_db;" "$new_conn"

  # Step 5: Compare oracle-new against source DB
  echo ""
  echo "=== Step 5: Comparing oracle-new against source DB ==="
  docker exec "$container" sqlplus -s "$sqlplus_conn" \
    @/scripts/compare.sql "$compare_pattern" "$compare_data_pattern"

  echo ""
  echo "=== Roundtrip test complete ==="
  echo "Results saved to: $output_dir"
}

cmd_usage() {
  echo "Usage: $0 <command> [args]"
  echo ""
  echo "Commands:"
  echo "  start                         Start Oracle containers and create database links"
  echo "  start-single [CONTAINER] [PORT]"
  echo "                                Start a single Oracle container (default: oracle-new on port 1522)"
  echo "  stop                          Stop and remove containers and network"
  echo "  status                        Show container status"
  echo "  create-link NAME CONN [CONTAINER] [SOURCE_CONN]"
  echo "                                Create a database link to any Oracle database"
  echo "                                NAME: link name (e.g., staging_db)"
  echo "                                CONN: target connection user/pass@//host:port/service"
  echo "  migrate FILE [CONTAINER] [CONN]"
  echo "                                Run SQL migrations via docker exec"
  echo "  migrate-orig [CONN]           Parse SQL files and run migrations on oracle-orig"
  echo "  migrate-new [CONN]            Process *-PROD-tables.sql files and run on oracle-new"
  echo "  generate-prod-tables [--force] Generate *-PROD-tables.sql and .yaml from all schema dirs"
  echo "  metadata-to-ddl FILE [OUT]    Convert XMDM_CONF INSERTs to DDL with COMMENT metadata"
  echo "  compare [PATTERN] [DATA_PATTERN] Compare tables matching pattern"
  echo "                                PATTERN: schema/table for structure comparison (default: GUI_XMDM%)"
  echo "                                DATA_PATTERN: schema.table for data comparison (default: GUI_XMDM.XMDM_CONF%)"
  echo "                                Examples:"
  echo "                                  compare                              - Defaults for both patterns"
  echo "                                  compare GUI_XMDM% NONE               - Structure only, no data comparison"
  echo "  compare-desired-state [OUT] [CONN] [SCHEMA] Generate schema migrations from DESIRED_STATE vs DB"
  echo "                                Schema pattern supports SQL LIKE syntax (e.g., GUI_XMDM%)"
  echo "  sync [OUT] [CONN] [SCHEMA]    Generate combined schema+data migrations from DESIRED_STATE vs DB"
  echo "                                Uses new unified sync command for both DDL and data changes"
  echo "  test-sync                     End-to-end test of sync with --ddls-from @database"
  echo "                                Tests UPDATE generation (not duplicate INSERTs)"
  echo "  dump-unified CONN [DDLS] [DATA] [OUT] [FILTER]"
  echo "                                Export DDL and data using unified filter (combined processing)"
  echo "                                CONN: user/pass@//host:port/service (required)"
  echo "                                DDLS: schema pattern (default: %)"
  echo "                                DATA: schema.table pattern (default: %.XMDM_CONF%)"
  echo "                                Uses unified-dump-filter.jq by default"
  echo "  roundtrip CONN [DDLS] [DATA] [CMP_PATTERN] [CMP_DATA]"
  echo "                                End-to-end roundtrip test: dump -> apply -> compare"
  echo "                                CONN: source connection user/pass@//host:port/service (required)"
  echo "                                DDLS: schema pattern for DDL export (default: %)"
  echo "                                DATA: schema.table pattern for data export (default: %.XMDM_CONF%)"
  echo "                                CMP_PATTERN: schema pattern for comparison (default: GUI_XMDM%)"
  echo "                                CMP_DATA: data pattern for comparison (default: GUI_XMDM.XMDM_CONF%)"
  echo "                                Tests information preservation through dump/migrate cycle"
  echo "  apply-desired-state [CONTAINER] [CONN] [DIR]"
  echo "                                Apply DESIRED_STATE DDL and metadata to a container"
  echo "                                CONTAINER: target container (default: oracle-orig)"
  echo "                                CONN: connection string (default: system/orakel@//localhost:1521/FREEPDB1)"
  echo "                                DIR: DESIRED_STATE directory (default: script_dir/DESIRED_STATE)"
  echo "  roundtrip-test [DIR] [CMP_PATTERN] [CMP_DATA]"
  echo "                                Full roundtrip test using DESIRED_STATE files:"
  echo "                                1. Start containers (if needed)"
  echo "                                2. Apply DESIRED_STATE to oracle-orig (DDL + metadata)"
  echo "                                3. Dump from oracle-orig, apply to oracle-new"
  echo "                                4. Compare oracle-new vs oracle-orig"
  echo "                                DIR: DESIRED_STATE directory (default: script_dir/DESIRED_STATE)"
  echo "                                CMP_PATTERN: schema pattern for comparison (default: GUI_XMDM%)"
  echo "                                CMP_DATA: data pattern for comparison (default: GUI_XMDM.XMDM_CONF%)"
  echo ""
  echo "Environment variables:"
  echo "  DEFAULT_CONN_STRING  Default connection string for Oracle"
  echo "                       Format: user/pass@//host:port/service"
  echo "                       Current: $DEFAULT_CONN_STRING"
  echo "  DEFAULT_CONTAINER    Default Docker container for docker exec (default: oracle-orig)"
  echo "  ORACLE_PASSWORD      Oracle system password (default: orakel)"
  echo "  SEQUALA_CMD          Command to run sequala CLI (default: 'sbt cli/run')"
  echo "                       Examples: 'java -jar sequala.jar', 'sbt cli/run'"
  echo ""
  echo "Connection details (after start):"
  local service
  service=$(parse_conn_string "$DEFAULT_CONN_STRING" service)
  echo "  $ORIG_CONTAINER: localhost:1521/$service (user: $APP_USER)"
  echo "  $NEW_CONTAINER:  localhost:1522/$service (user: $APP_USER)"
}

# Only execute case statement if script is run directly (not sourced)
# Check if script name appears in $0 (when run directly) vs BASH_SOURCE (when sourced)
if [[ "${BASH_SOURCE[0]}" == "${0}" ]] || [[ "$0" == *"oracle-comparison.sh"* ]]; then
  case "${1:-}" in
    start)
      cmd_start
      ;;
    start-single)
      cmd_start_single "$2" "$3"
      ;;
    stop)
      cmd_stop
      ;;
    status)
      cmd_status
      ;;
    create-link)
      cmd_create_link "$2" "$3" "$4" "$5"
      ;;
    migrate)
      cmd_migrate "$2" "$3" "$4"
      ;;
    migrate-orig)
      cmd_migrate_orig
      ;;
    migrate-new)
      cmd_migrate_new
      ;;
    generate-prod-tables)
      cmd_generate_prod_tables "$2"
      ;;
    compare)
      cmd_compare "$2" "$3" "$4" "$5"
      ;;
    compare-desired-state)
      cmd_compare_desired_state "$2" "$3" "$4"
      ;;
    sync)
      cmd_sync "$2" "$3" "$4"
      ;;
    test-sync)
      cmd_test_sync
      ;;
    metadata-to-ddl)
      cmd_metadata_to_ddl "$2" "$3"
      ;;
    dump-unified)
      cmd_dump_unified "$2" "$3" "$4" "$5" "$6"
      ;;
    roundtrip)
      cmd_roundtrip "$2" "$3" "$4" "$5" "$6"
      ;;
    apply-desired-state)
      cmd_apply_desired_state "$2" "$3" "$4"
      ;;
    roundtrip-test)
      cmd_roundtrip_test "$2" "$3" "$4"
      ;;
    *)
      cmd_usage
      exit 1
      ;;
  esac
fi
