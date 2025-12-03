# Sequala

A versatile SQL parser and desired-state schema migration tool for Scala.

Built with [fastparse](http://www.lihaoyi.com/fastparse/).

## Features

- **SQL parsing**: Parse SQL into a typed AST, with multi-dialect support (PostgreSQL, Oracle, ANSI SQL)
- **JSON output with jq queries**: Export parsed SQL as JSON and run jq queries for analysis
- **Desired-state migrations**: Define your target schema in SQL files, Sequala computes the diff and generates migration SQL
- **Database introspection**: Extract current schema from live databases
- **Type-safe schema model**: Scala 3 union types ensure dialect-specific features stay where they belong
- **Transactional migrations**: Full rollback support on PostgreSQL, best-effort on Oracle

## SQL Parser

### CLI Usage

```bash
# Parse SQL files and output as text
sequala postgres schema/*.sql

# Parse with JSON output
sequala oracle --output json legacy-schema.sql

# Parse multiple files with glob patterns
sequala postgres --output json "src/**/*.sql"

# Run jq queries on parsed SQL
sequala postgres --output 'jq(.fileResults[].statementResults[].parseResult.value)' schema.sql

# Use jq query from file
sequala postgres --output 'jq-file:analyze.jq' schema.sql

# Write output to file
sequala postgres --output json --write-to parsed.json schema.sql
```

### Programmatic Usage

```scala
import sequala.postgres.PostgresSQL
import sequala.oracle.OracleSQL
import sequala.ansi.ANSISQL

// Parse a single statement
val result = PostgresSQL("SELECT * FROM users WHERE id = 1")

// Parse multiple statements
val statements = PostgresSQL.parseStatements("""
  CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);
  CREATE INDEX idx_name ON users(name);
""")

// Parse with error recovery (returns all statements, marking unparseable ones)
val allStatements = OracleSQL.parseAll(sqlContent)
```

## Schema Migrations

### CLI Usage

```bash
# Generate migration plan (dry-run)
sequala migrate plan \
  --source desired-schema.sql \
  --database jdbc:postgresql://localhost:5432/mydb \
  --user postgres \
  --password secret

# Apply migrations
sequala migrate apply \
  --source desired-schema.sql \
  --database jdbc:postgresql://localhost:5432/mydb \
  --auto-approve

# Inspect current database schema
sequala migrate inspect \
  --database jdbc:postgresql://localhost:5432/mydb \
  --output current-schema.sql

# Diff two SQL files
sequala migrate diff \
  --from current.sql \
  --to desired.sql
```

### Programmatic Usage

```scala
import sequala.migrate._
import sequala.migrate.postgres._
import sequala.postgres.PostgresSQL
import sequala.converter.SchemaBuilder

// Parse desired schema
val desiredDDL = """
  CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
"""
val desiredTables = SchemaBuilder.build(PostgresSQL.parseStatements(desiredDDL))

// Inspect current database
val inspector = new PostgresSchemaInspector()
val currentTables = inspector.inspectTables(connection, schema = "public")

// Compute and execute migration
val changes = SchemaDiffer.diff(currentTables, desiredTables)
val steps = PostgresMigrationGenerator.generate(changes)
val result = PostgresMigrationExecutor.execute(connection, steps)
```

## Supported Dialects

### PostgreSQL
- Types: SERIAL, BIGSERIAL, UUID, JSON, JSONB, arrays, geometric types, INET/CIDR
- Features: Table inheritance, partitioning, partial indexes, generated columns
- Migrations: Full transactional DDL with automatic rollback

### Oracle
- Types: VARCHAR2 (BYTE/CHAR), NUMBER, BINARY_FLOAT/DOUBLE, XMLTYPE, intervals
- Features: Tablespaces, storage clauses, virtual columns, invisible columns
- Migrations: Auto-commit with best-effort rollback via reverse statements

### ANSI SQL
- Standard SQL types and DDL as baseline for custom dialects

## Modules

| Module | Description |
|--------|-------------|
| `sequala-schema` | Dialect-agnostic schema model (Table, Column, Constraint, Index) |
| `sequala-schema-postgres` | PostgreSQL-specific types and options |
| `sequala-schema-oracle` | Oracle-specific types and options |
| `sequala-parser` | ANSI SQL parser |
| `sequala-parser-postgres` | PostgreSQL dialect parser |
| `sequala-parser-oracle` | Oracle dialect parser |
| `sequala-migrate` | Schema differ and migration engine |
| `sequala-migrate-postgres` | PostgreSQL inspector, generator, executor |
| `sequala-migrate-oracle` | Oracle inspector, generator, executor |
| `sequala-cli` | Command-line interface |

## Installation

```scala
// For parsing only
libraryDependencies += "dev.mauch" %% "sequala-parser-postgres" % "1.7.1"

// For migrations
libraryDependencies += "dev.mauch" %% "sequala-migrate-postgres" % "1.7.1"
```

## How Migrations Work

```
Desired DDL          Current Database
     │                      │
     ▼                      ▼
  Parser              Inspector
     │                      │
     └──────────┬───────────┘
                ▼
          SchemaDiffer
                │
                ▼
          SchemaDiff[]
      (CreateTable, DropTable,
       AlterTable, CreateIndex...)
                │
                ▼
       MigrationGenerator
                │
                ▼
        MigrationStep[]
         (SQL + comments)
                │
        ┌───────┴───────┐
        ▼               ▼
   View (CLI)     Execute (DB)
```

## Detected Changes

- **Tables**: Create, drop
- **Columns**: Add, drop, modify (type, nullability, default), rename
- **Constraints**: Primary key, foreign key, unique, check
- **Indexes**: Create, drop

## Version History

See [CHANGELOG.md](CHANGELOG.md) for detailed version history and release notes.

## Attribution

Sequala originated as a fork of [Sparsity](https://github.com/UBOdin/sparsity) by UBOdin, licensed under Apache 2.0. The codebase has since been substantially rewritten.

## License

Apache 2.0
