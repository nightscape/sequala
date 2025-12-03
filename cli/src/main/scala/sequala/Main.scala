package sequala

import mainargs.{arg, main, Leftover, ParserForMethods, TokensReader}
import sequala.parse.{*, given}
import sequala.common.parser.SQLBaseObject
import sequala.migrate.cli.MigrateSubcommands

object Subcommands:

  @main(doc = "Parse SQL files using the specified dialect")
  def parse(
    @arg(positional = true, doc = "SQL dialect to use (oracle, ansi, postgres)")
    dialect: SQLBaseObject,
    @arg(doc =
      "Output format. Format: text|json|jq(query)|jq-file:path. Example: --output jq(.files[].statements[]) or --output jq-file:pr-checker.jq"
    )
    output: OutputFormat = TextFormat,
    @arg(doc = "Output destination. Example: --write-to console or --write-to output.json")
    writeTo: OutputDestination = ConsoleDestination,
    @arg(doc = "SQL file(s) or glob pattern(s) to parse (e.g., *.sql)")
    filePatterns: Leftover[String]
  ): Unit =
    ParseRunner.run(dialect, output, writeTo, filePatterns.value)

  @main(doc = "Plan migration: Parse DDL, inspect database, compute diff, output SQL")
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
    MigrateSubcommands.plan(source, database, user, password, schema, dialect, config, env, format, pretty)

  @main(doc = "Apply migration: Execute the planned changes against the database")
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
    MigrateSubcommands.apply(
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

  @main(doc = "Inspect database: Extract current schema as SQL DDL")
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
    MigrateSubcommands.inspect(database, user, password, schema, dialect, config, env, output, tableFilter, pretty)

  @main(doc = "Diff: Compare two SQL DDL files and output migration SQL")
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
    MigrateSubcommands.diff(from, to, dialect, format, pretty)

object Main:
  def main(args: Array[String]): Unit =
    ParserForMethods(Subcommands).runOrExit(args.toIndexedSeq)
