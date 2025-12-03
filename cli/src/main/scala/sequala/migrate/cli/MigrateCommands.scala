package sequala.migrate.cli

import mainargs.{arg, main, Leftover, ParserForClass, ParserForMethods, TokensReader}
import java.io.File

@main(doc = "Plan migration: Parse DDL, inspect database, compute diff, output SQL")
case class PlanCommand(
  @arg(doc = "SQL DDL file(s) describing desired schema")
  source: String,
  @arg(doc = "JDBC database URL (e.g., jdbc:postgresql://localhost:5432/mydb)")
  database: Option[String] = None,
  @arg(doc = "Database user")
  user: Option[String] = None,
  @arg(doc = "Database password")
  password: Option[String] = None,
  @arg(doc = "Database schema name (default: public for Postgres, SYSTEM for Oracle)")
  schema: Option[String] = None,
  @arg(doc = "Database dialect: postgres, oracle, generic")
  dialect: Option[String] = None,
  @arg(doc = "Configuration file path")
  config: Option[String] = None,
  @arg(doc = "Environment name from config file")
  env: Option[String] = None,
  @arg(doc = "Output format: text (default), sql, json")
  format: String = "text",
  @arg(doc = "Pretty print SQL with newlines and indentation")
  pretty: Boolean = false
)

@main(doc = "Apply migration: Execute the planned changes against the database")
case class ApplyCommand(
  @arg(doc = "SQL DDL file(s) describing desired schema")
  source: String,
  @arg(doc = "JDBC database URL (e.g., jdbc:postgresql://localhost:5432/mydb)")
  database: Option[String] = None,
  @arg(doc = "Database user")
  user: Option[String] = None,
  @arg(doc = "Database password")
  password: Option[String] = None,
  @arg(doc = "Database schema name (default: public for Postgres, SYSTEM for Oracle)")
  schema: Option[String] = None,
  @arg(doc = "Database dialect: postgres, oracle, generic")
  dialect: Option[String] = None,
  @arg(doc = "Configuration file path")
  config: Option[String] = None,
  @arg(doc = "Environment name from config file")
  env: Option[String] = None,
  @arg(doc = "Dry run: show SQL without executing")
  dryRun: Boolean = false,
  @arg(doc = "Auto-approve: skip confirmation prompt")
  autoApprove: Boolean = false,
  @arg(doc = "Transactional: wrap in transaction (if supported)")
  transactional: Boolean = true,
  @arg(doc = "Pretty print SQL with newlines and indentation")
  pretty: Boolean = false
)

@main(doc = "Inspect database: Extract current schema as SQL DDL")
case class InspectCommand(
  @arg(doc = "JDBC database URL (e.g., jdbc:postgresql://localhost:5432/mydb)")
  database: Option[String] = None,
  @arg(doc = "Database user")
  user: Option[String] = None,
  @arg(doc = "Database password")
  password: Option[String] = None,
  @arg(doc = "Database schema name (default: public for Postgres, SYSTEM for Oracle)")
  schema: Option[String] = None,
  @arg(doc = "Database dialect: postgres, oracle, generic")
  dialect: Option[String] = None,
  @arg(doc = "Configuration file path")
  config: Option[String] = None,
  @arg(doc = "Environment name from config file")
  env: Option[String] = None,
  @arg(doc = "Output file (default: stdout)")
  output: Option[String] = None,
  @arg(doc = "Table name filter (glob pattern, e.g., 'user*')")
  tableFilter: Option[String] = None,
  @arg(doc = "Pretty print SQL with newlines and indentation")
  pretty: Boolean = false
)

@main(doc = "Diff: Compare two SQL DDL files and output migration SQL")
case class DiffCommand(
  @arg(doc = "Source DDL file (current state)")
  from: String,
  @arg(doc = "Target DDL file (desired state)")
  to: String,
  @arg(doc = "SQL dialect: postgres, oracle, generic (default)")
  dialect: String = "generic",
  @arg(doc = "Output format: text (default), sql, json")
  format: String = "text",
  @arg(doc = "Pretty print SQL with newlines and indentation")
  pretty: Boolean = false
)

object MigrateSubcommands:
  @main
  def plan(
    @arg(doc = "SQL DDL file(s) describing desired schema")
    source: String,
    @arg(doc = "JDBC database URL")
    database: Option[String] = None,
    @arg(doc = "Database user")
    user: Option[String] = None,
    @arg(doc = "Database password")
    password: Option[String] = None,
    @arg(doc = "Database schema name")
    schema: Option[String] = None,
    @arg(doc = "Database dialect: postgres, oracle, generic")
    dialect: Option[String] = None,
    @arg(doc = "Configuration file path")
    config: Option[String] = None,
    @arg(doc = "Environment name from config file")
    env: Option[String] = None,
    @arg(doc = "Output format: text, sql, json")
    format: String = "text",
    @arg(doc = "Pretty print SQL with newlines and indentation")
    pretty: Boolean = false
  ): Unit =
    val cmd = PlanCommand(source, database, user, password, schema, dialect, config, env, format, pretty)
    MigrateRunner.runPlan(cmd)

  @main
  def apply(
    @arg(doc = "SQL DDL file(s) describing desired schema")
    source: String,
    @arg(doc = "JDBC database URL")
    database: Option[String] = None,
    @arg(doc = "Database user")
    user: Option[String] = None,
    @arg(doc = "Database password")
    password: Option[String] = None,
    @arg(doc = "Database schema name")
    schema: Option[String] = None,
    @arg(doc = "Database dialect: postgres, oracle, generic")
    dialect: Option[String] = None,
    @arg(doc = "Configuration file path")
    config: Option[String] = None,
    @arg(doc = "Environment name from config file")
    env: Option[String] = None,
    @arg(doc = "Dry run: show SQL without executing")
    dryRun: Boolean = false,
    @arg(doc = "Auto-approve: skip confirmation prompt")
    autoApprove: Boolean = false,
    @arg(doc = "Transactional: wrap in transaction")
    transactional: Boolean = true,
    @arg(doc = "Pretty print SQL with newlines and indentation")
    pretty: Boolean = false
  ): Unit =
    val cmd =
      ApplyCommand(
        source,
        database,
        user,
        password,
        schema,
        dialect,
        config,
        env,
        dryRun,
        autoApprove,
        transactional,
        pretty
      )
    MigrateRunner.runApply(cmd)

  @main
  def inspect(
    @arg(doc = "JDBC database URL")
    database: Option[String] = None,
    @arg(doc = "Database user")
    user: Option[String] = None,
    @arg(doc = "Database password")
    password: Option[String] = None,
    @arg(doc = "Database schema name")
    schema: Option[String] = None,
    @arg(doc = "Database dialect: postgres, oracle, generic")
    dialect: Option[String] = None,
    @arg(doc = "Configuration file path")
    config: Option[String] = None,
    @arg(doc = "Environment name from config file")
    env: Option[String] = None,
    @arg(doc = "Output file (stdout if not specified)")
    output: Option[String] = None,
    @arg(doc = "Table name filter (glob pattern)")
    tableFilter: Option[String] = None,
    @arg(doc = "Pretty print SQL with newlines and indentation")
    pretty: Boolean = false
  ): Unit =
    val cmd = InspectCommand(database, user, password, schema, dialect, config, env, output, tableFilter, pretty)
    MigrateRunner.runInspect(cmd)

  @main
  def diff(
    @arg(doc = "Source DDL file (current state)")
    from: String,
    @arg(doc = "Target DDL file (desired state)")
    to: String,
    @arg(doc = "SQL dialect: postgres, oracle, generic")
    dialect: String = "generic",
    @arg(doc = "Output format: text, sql, json")
    format: String = "text",
    @arg(doc = "Pretty print SQL with newlines and indentation")
    pretty: Boolean = false
  ): Unit =
    val cmd = DiffCommand(from, to, dialect, format, pretty)
    MigrateRunner.runDiff(cmd)
