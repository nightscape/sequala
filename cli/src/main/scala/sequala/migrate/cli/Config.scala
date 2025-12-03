package sequala.migrate.cli

import com.typesafe.config.{Config as TypesafeConfig, ConfigFactory, ConfigValueFactory}
import java.io.File
import java.nio.file.{Path, Paths}
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

enum DatabaseDialect:
  case Postgres
  case Oracle
  case Generic

  def toJdbcPrefix: String = this match
    case Postgres => "jdbc:postgresql"
    case Oracle => "jdbc:oracle:thin"
    case Generic => "jdbc"

object DatabaseDialect:
  def fromString(s: String): Either[String, DatabaseDialect] =
    s.toLowerCase match
      case "postgres" | "postgresql" => Right(Postgres)
      case "oracle" => Right(Oracle)
      case "generic" | "ansi" => Right(Generic)
      case other => Left(s"Unknown dialect: $other. Valid dialects: postgres, oracle, generic")

  def fromJdbcUrl(url: String): DatabaseDialect =
    val lower = url.toLowerCase
    if lower.startsWith("jdbc:postgresql") then Postgres
    else if lower.startsWith("jdbc:oracle") then Oracle
    else Generic

case class DatabaseConfig(
  url: String,
  user: Option[String] = None,
  password: Option[String] = None,
  schema: String = "public",
  dialect: DatabaseDialect = DatabaseDialect.Postgres
):
  def jdbcUrl: String = url

  def inferredDialect: DatabaseDialect =
    if dialect != DatabaseDialect.Generic then dialect
    else DatabaseDialect.fromJdbcUrl(url)

case class MigrationConfig(
  source: Option[String] = None,
  database: DatabaseConfig,
  dryRun: Boolean = false,
  autoApprove: Boolean = false
)

object ConfigLoader:
  private val defaultConfigNames = Seq("sequala.conf", "sequala.json", "application.conf")

  def loadConfig(configPath: Option[String], environment: Option[String]): Either[String, TypesafeConfig] =
    val baseConfig = configPath match
      case Some(path) =>
        val file = new File(path)
        if !file.exists() then return Left(s"Config file not found: $path")
        Try(ConfigFactory.parseFile(file).resolve()) match
          case Success(c) => c
          case Failure(e) => return Left(s"Failed to parse config file: ${e.getMessage}")
      case None =>
        findDefaultConfig() match
          case Some(file) =>
            Try(ConfigFactory.parseFile(file).resolve()) match
              case Success(c) => c
              case Failure(e) => return Left(s"Failed to parse config file: ${e.getMessage}")
          case None => ConfigFactory.empty()

    val envConfig = environment match
      case Some(env) if baseConfig.hasPath(s"environments.$env") =>
        baseConfig.getConfig(s"environments.$env").withFallback(baseConfig)
      case Some(env) if baseConfig.hasPath(env) =>
        baseConfig.getConfig(env).withFallback(baseConfig)
      case _ => baseConfig

    Right(envConfig)

  def loadDatabaseConfig(
    config: TypesafeConfig,
    urlOverride: Option[String] = None,
    userOverride: Option[String] = None,
    passwordOverride: Option[String] = None,
    schemaOverride: Option[String] = None,
    dialectOverride: Option[String] = None
  ): Either[String, DatabaseConfig] =
    val url = urlOverride
      .orElse(getOptionalString(config, "database.url"))
      .orElse(getOptionalString(config, "url"))
      .orElse(Option(System.getenv("DATABASE_URL")))

    url match
      case None => Left("Database URL is required. Provide via --database, config file, or DATABASE_URL env var")
      case Some(jdbcUrl) =>
        val user = userOverride
          .orElse(getOptionalString(config, "database.user"))
          .orElse(getOptionalString(config, "user"))
          .orElse(Option(System.getenv("DATABASE_USER")))

        val password = passwordOverride
          .orElse(getOptionalString(config, "database.password"))
          .orElse(getOptionalString(config, "password"))
          .orElse(Option(System.getenv("DATABASE_PASSWORD")))

        val schema = schemaOverride
          .orElse(getOptionalString(config, "database.schema"))
          .orElse(getOptionalString(config, "schema"))
          .orElse(Option(System.getenv("DATABASE_SCHEMA")))
          .getOrElse(defaultSchemaForDialect(DatabaseDialect.fromJdbcUrl(jdbcUrl)))

        val dialectStr = dialectOverride
          .orElse(getOptionalString(config, "database.dialect"))
          .orElse(getOptionalString(config, "dialect"))

        val dialect = dialectStr match
          case Some(d) =>
            DatabaseDialect.fromString(d) match
              case Right(dialect) => dialect
              case Left(err) => return Left(err)
          case None => DatabaseDialect.fromJdbcUrl(jdbcUrl)

        Right(DatabaseConfig(url = jdbcUrl, user = user, password = password, schema = schema, dialect = dialect))

  private def defaultSchemaForDialect(dialect: DatabaseDialect): String =
    dialect match
      case DatabaseDialect.Postgres => "public"
      case DatabaseDialect.Oracle => "SYSTEM"
      case DatabaseDialect.Generic => "public"

  private def getOptionalString(config: TypesafeConfig, path: String): Option[String] =
    if config.hasPath(path) then Some(resolveEnvVars(config.getString(path)))
    else None

  private def resolveEnvVars(value: String): String =
    val envVarPattern = """\$\{(\w+)\}""".r
    val envVarPatternAlt = """\$(\w+)""".r

    val resolved1 = envVarPattern.replaceAllIn(
      value,
      m =>
        val envName = m.group(1)
        Option(System.getenv(envName)).getOrElse(m.matched)
    )

    envVarPatternAlt.replaceAllIn(
      resolved1,
      m =>
        val envName = m.group(1)
        Option(System.getenv(envName)).getOrElse(m.matched)
    )

  private def findDefaultConfig(): Option[File] =
    val currentDir = Paths.get(".").toAbsolutePath.normalize
    defaultConfigNames.map(name => currentDir.resolve(name).toFile).find(_.exists())
