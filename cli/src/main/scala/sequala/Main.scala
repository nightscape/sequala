package sequala

import mainargs.{arg, main, Flag, Leftover, ParserForClass, ParserForMethods, TokensReader}
import java.nio.file.{Path, Paths}
import sequala.common.{AnsiDialect, Dialect, Dialects, OracleDialect, PostgresDialect, SchemaSource, given}
import sequala.parse.{
  ConsoleDestination,
  OutputDestination,
  OutputFormatFactory,
  OutputFormatFactoryReader,
  ParseRunner,
  RenderRunner,
  TextFormat,
  TransformFactory,
  TransformFactoryReader,
  Transforms
}
import sequala.migrate.cli.{DataDiffRunner, MigrateRunner, SyncRunner}

@main
case class DbConfig(
  @arg(doc = "JDBC database URL")
  database: Option[String] = None,
  @arg(doc = "Database user")
  user: Option[String] = None,
  @arg(doc = "Database password")
  password: Option[String] = None,
  @arg(doc = "Schema pattern (SQL LIKE: % any, _ single char)")
  schema: Option[String] = None,
  @arg(doc = "Database dialect: postgres, oracle, ansi")
  dialect: Option[Dialect] = None,
  @arg(doc = "Configuration file path")
  config: Option[Path] = None,
  @arg(doc = "Environment name from config file")
  env: Option[String] = None
)

given ParserForClass[DbConfig] = ParserForClass[DbConfig]

given OptionPathReader: TokensReader.Simple[Option[Path]] with
  def shortName = "path"
  def read(strs: Seq[String]): Either[String, Option[Path]] =
    Right(strs.headOption.map(Paths.get(_)))

given PathReader: TokensReader.Simple[Path] with
  def shortName = "path"
  def read(strs: Seq[String]): Either[String, Path] =
    strs.headOption.map(s => Right(Paths.get(s))).getOrElse(Left("Missing path"))

object Subcommands:

  @main(doc = "Parse SQL files using the specified dialect")
  def parse(
    @arg(positional = true, doc = "SQL dialect to use (oracle, ansi, postgres)")
    dialect: Dialect,
    @arg(doc =
      "Output format: text|sql|json|jq(query)|jq-file:path. Example: --output sql or --output jq-file:transform.jq"
    )
    output: OutputFormatFactory = _ => TextFormat,
    @arg(doc =
      "Output destination: console, file.sql, or directory/ (trailing slash). Example: --write-to output/ for @write directives"
    )
    writeTo: OutputDestination = ConsoleDestination,
    @arg(doc = "Pretty print SQL with newlines and indentation")
    pretty: Boolean = false,
    @arg(doc = "Apply all built-in simplifications (equivalent to --transform=filter,drop-optimize,dedupe,sort)")
    simplify: Flag,
    @arg(doc =
      "Apply specific transforms (comma-separated). Examples: --transform=filter,dedupe,sort or --transform=jq:custom or --transform=jq:./my-script.jq"
    )
    transform: Option[String] = None,
    @arg(doc = "Capture SQL comments (-- @KEY: value) into sourceComment on tables and columns (Oracle only)")
    withComments: Boolean = false,
    @arg(doc = "SQL file(s) or glob pattern(s) to parse (e.g., *.sql)")
    filePatterns: Leftover[String]
  ): Unit =
    val transformTokens =
      if simplify.value then Some("filter,drop-optimize,dedupe,sort")
      else transform

    val transforms = transformTokens match
      case Some(tokens) =>
        Transforms.parseTransformTokens(Seq(tokens)) match
          case Right(parsed) => parsed
          case Left(err) =>
            System.err.println(s"Error parsing transforms: $err")
            sys.exit(1)
      case None => Seq.empty

    ParseRunner.run(dialect, output, writeTo, pretty, transforms, filePatterns.value, withComments)

  @main(doc = "Plan migration: Parse DDL, inspect database, compute diff, output SQL")
  def plan(
    @arg(doc = "SQL DDL file(s) or glob pattern describing desired schema (e.g., *.sql)")
    source: String,
    db: DbConfig,
    @arg(doc = "Output format: text, sql, json")
    format: String = "text",
    @arg(doc = "Pretty print SQL with newlines and indentation")
    pretty: Boolean = false,
    @arg(doc = "Output destination: console, file.sql, or directory/ (trailing slash)")
    writeTo: OutputDestination = ConsoleDestination,
    @arg(doc = "Apply transforms to migration output (comma-separated). Example: --transform=exclude:DropTable")
    transform: Option[TransformFactory] = None,
    @arg(doc = "JQ filter file to transform source DDL before comparison (e.g., generate derived tables)")
    sourceTransform: Option[TransformFactory] = None
  ): Unit =
    MigrateRunner.runPlan(source, db, format, pretty, writeTo, transform, sourceTransform)

  @main(doc = "Apply migration: Execute the planned changes against the database")
  def apply(
    @arg(doc = "SQL DDL file(s) describing desired schema")
    source: String,
    db: DbConfig,
    @arg(doc = "Dry run: show SQL without executing")
    dryRun: Boolean = false,
    @arg(doc = "Auto-approve: skip confirmation prompt")
    autoApprove: Boolean = false,
    @arg(doc = "Transactional: wrap in transaction")
    transactional: Boolean = true,
    @arg(doc = "Pretty print SQL with newlines and indentation")
    pretty: Boolean = false
  ): Unit =
    MigrateRunner.runApply(source, db, dryRun, autoApprove, transactional, pretty)

  @main(doc = "Inspect database: Extract current schema as SQL DDL")
  def inspect(
    db: DbConfig,
    @arg(doc = "Output file (stdout if not specified)")
    output: Option[Path] = None,
    @arg(doc = "Table name filter (glob pattern)")
    tableFilter: Option[String] = None,
    @arg(doc = "Pretty print SQL with newlines and indentation")
    pretty: Boolean = false
  ): Unit =
    MigrateRunner.runInspect(db, output, tableFilter, pretty)

  @main(doc = "Render: Convert JSON statements to SQL")
  def render(
    @arg(positional = true, doc = "SQL dialect to use (oracle, ansi, postgres)")
    dialect: Dialect,
    @arg(doc = "Input JSON file containing statement array")
    input: String,
    @arg(doc = "Pretty print SQL with newlines and indentation")
    pretty: Boolean = false,
    @arg(doc = "Output destination: console or file.sql")
    writeTo: OutputDestination = ConsoleDestination
  ): Unit =
    RenderRunner.run(dialect, input, pretty, writeTo)

  @main(doc = "Diff: Compare two SQL DDL files and output migration SQL")
  def diff(
    @arg(doc = "Source DDL file (current state)")
    from: String,
    @arg(doc = "Target DDL file (desired state)")
    to: String,
    @arg(doc = "SQL dialect: postgres, oracle, ansi")
    dialect: Dialect = AnsiDialect,
    @arg(doc = "Output format: text, sql, json")
    format: String = "text",
    @arg(doc = "Pretty print SQL with newlines and indentation")
    pretty: Boolean = false,
    @arg(doc = "Output destination: console, file.sql, or directory/ (trailing slash)")
    writeTo: OutputDestination = ConsoleDestination,
    @arg(doc = "Apply transforms to migration output (comma-separated). Example: --transform=exclude:DropTable")
    transform: Option[TransformFactory] = None
  ): Unit =
    MigrateRunner.runDiff(from, to, dialect, format, pretty, writeTo, transform)

  @main(doc = "Data-diff: Compare INSERT statements and generate migration DMLs")
  def dataDiff(
    @arg(doc = "Source data file (current state)")
    from: String,
    @arg(doc = "Target data file (desired state)")
    to: String,
    @arg(doc = "SQL dialect: postgres, oracle, ansi")
    dialect: Dialect = AnsiDialect,
    @arg(doc = "Key columns for row matching (comma-separated, e.g., 'id' or 'schema,table')")
    key: Option[String] = None,
    @arg(doc = "DDL file for primary key inference (alternative to --key)")
    ddl: Option[String] = None,
    @arg(doc = "Output format: sql, json")
    format: String = "sql",
    @arg(doc = "Pretty print SQL with newlines and indentation")
    pretty: Boolean = false,
    @arg(doc = "Output destination: console or file.sql")
    writeTo: OutputDestination = ConsoleDestination,
    @arg(doc = "Generate DELETE statements for rows missing in target")
    withDeletes: Boolean = true
  ): Unit =
    DataDiffRunner.runDiff(from, to, dialect, key, ddl, format, pretty, writeTo, withDeletes)

  @main(doc = "Data-plan: Compare INSERT data with database and generate migration DMLs")
  def dataPlan(
    @arg(doc = "Target data file (desired state)")
    source: String,
    db: DbConfig,
    @arg(doc = "Key columns for row matching (comma-separated)")
    key: Option[String] = None,
    @arg(doc = "DDL file for primary key inference")
    ddl: Option[String] = None,
    @arg(doc = "Table filter pattern (only compare tables matching this pattern)")
    tableFilter: Option[String] = None,
    @arg(doc = "Output format: sql, json")
    format: String = "sql",
    @arg(doc = "Pretty print SQL")
    pretty: Boolean = false,
    @arg(doc = "Output destination: console or file.sql")
    writeTo: OutputDestination = ConsoleDestination,
    @arg(doc = "Generate DELETE statements for rows missing in target")
    withDeletes: Boolean = true
  ): Unit =
    DataDiffRunner.runPlan(source, db, key, ddl, tableFilter, format, pretty, writeTo, withDeletes)

  @main(doc = "Dump: Export DDL and/or data from database")
  def dump(
    db: DbConfig,
    @arg(doc = "Table pattern for DDL export (SQL LIKE syntax: % for any, _ for single char)")
    ddls: Option[String] = None,
    @arg(doc = "Schema.table pattern for data export (e.g., 'MYSCHEMA.%_CONF')")
    data: Option[String] = None,
    @arg(doc = "Output directory (default: current directory)")
    outputDir: Path = Paths.get("."),
    @arg(doc = "Batch size for data export (rows per INSERT)")
    batchSize: Int = 1000,
    @arg(doc = "Pretty print SQL (default: true)")
    pretty: Boolean = true,
    @arg(doc = "JQ filter file for DDL output transformation")
    ddlsFilter: Option[Path] = None,
    @arg(doc = "JQ filter file for data output transformation")
    dataFilter: Option[Path] = None,
    @arg(doc =
      "JQ filter file for combined DDL and data transformation (cannot be used with --ddls-filter or --data-filter)"
    )
    filter: Option[Path] = None
  ): Unit =
    MigrateRunner.runDump(db, ddls, data, outputDir, batchSize, pretty, ddlsFilter, dataFilter, filter)

  @main(doc = "Sync: Plan combined schema and data migration")
  def sync(
    @arg(doc = "Desired state file(s) - DDL and/or data (glob pattern)")
    desired: String,
    db: DbConfig,
    @arg(doc = "Key columns for data row matching (comma-separated)")
    key: Option[String] = None,
    @arg(doc = "Output format: text, sql, json")
    format: String = "text",
    @arg(doc = "Pretty print SQL")
    pretty: Boolean = false,
    @arg(doc = "Output destination: console, file.sql, or directory/")
    writeTo: OutputDestination = ConsoleDestination,
    @arg(doc = "JQ filter for source transformation before comparison")
    sourceTransform: Option[TransformFactory] = None,
    @arg(doc = "Apply transforms to migration output")
    transform: Option[TransformFactory] = None,
    @arg(doc = "Generate DELETE/DROP for missing items")
    withDeletes: Boolean = true,
    @arg(doc = "Table filter pattern")
    tableFilter: Option[String] = None,
    @arg(doc = "Additional DDL source for PK inference: file pattern or @database")
    ddlsFrom: Option[SchemaSource] = None
  ): Unit =
    SyncRunner.runSync(
      desired,
      db,
      key,
      format,
      pretty,
      writeTo,
      sourceTransform,
      transform,
      withDeletes,
      tableFilter,
      ddlsFrom
    )

object Main:
  private def initializeLogging(): Unit =
    import wvlet.log.{Logger, LogLevel}
    val level = sys.env.getOrElse("SEQUALA_LOG_LEVEL", "warn").toLowerCase match
      case "debug" => LogLevel.DEBUG
      case "info" => LogLevel.INFO
      case "warn" => LogLevel.WARN
      case "error" => LogLevel.ERROR
      case "trace" => LogLevel.TRACE
      case _ => LogLevel.WARN
    Logger.setDefaultLogLevel(level)

  def main(args: Array[String]): Unit =
    initializeLogging()
    ParserForMethods(Subcommands).runOrExit(args.toIndexedSeq)
