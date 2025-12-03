package sequala.common

import mainargs.TokensReader
import sequala.schema.{Statement, Table}

import java.io.File
import java.sql.Connection
import scala.io.Source
import scala.util.{Failure, Success, Try}

/** Represents a source of schema information (DDL tables with primary keys).
  *
  * This abstraction unifies file-based and database-based schema sources, allowing CLI commands to accept either:
  *   - File patterns: `./migrations/\*.sql`, `path/to/schema.sql`
  *   - Current database: `@database`, `@db`
  */
sealed trait SchemaSource:

  /** Resolve this schema source to a map of table name -> Table.
    *
    * @param dialect
    *   SQL dialect for parsing/inspecting
    * @param connection
    *   Database connection (required for CurrentDatabase, ignored for Files)
    * @param schemaPattern
    *   Schema pattern for database inspection (SQL LIKE syntax, e.g., "SOME_SCHEMA%")
    * @return
    *   Map of normalized table name to Table objects
    */
  def resolve(
    dialect: Dialect,
    connection: Option[Connection] = None,
    schemaPattern: Option[String] = None
  ): Either[String, Map[String, Table[?, ?, ?]]]

  protected def tablesToMap(tables: Seq[Table[?, ?, ?]]): Map[String, Table[?, ?, ?]] =
    val withSchema = tables.flatMap { t =>
      t.schema match
        case Some(s) => Some(s"${s.toUpperCase}.${t.name.toUpperCase}" -> t)
        case None => None
    }.toMap

    val withoutSchema = tables.map(t => t.name.toUpperCase -> t).toMap

    withSchema ++ withoutSchema

object SchemaSource:

  /** Schema from SQL files matching a glob pattern */
  case class Files(pattern: String) extends SchemaSource:

    def resolve(
      dialect: Dialect,
      connection: Option[Connection],
      schemaPattern: Option[String]
    ): Either[String, Map[String, Table[?, ?, ?]]] =
      val files = FilePatternResolver.resolve(pattern)
      if files.isEmpty then Left(s"No files match pattern: $pattern")
      else Right(tablesToMap(parseFiles(files, dialect)))

    private def parseFiles(files: Seq[File], dialect: Dialect): Seq[Table[?, ?, ?]] =
      val statements = files.flatMap { file =>
        Try(Source.fromFile(file).mkString) match
          case Failure(e) =>
            System.err.println(s"Warning: Failed to read ${file.getName}: ${e.getMessage}")
            Seq.empty
          case Success(content) =>
            dialect.parser.parseAll(content).flatMap(_.result.toOption)
      }
      dialect.tablesFromStatements(statements).map(_.asInstanceOf[Table[?, ?, ?]])

  /** Schema from the current database connection (the `--database` parameter) */
  case object CurrentDatabase extends SchemaSource:

    def resolve(
      dialect: Dialect,
      connection: Option[Connection],
      schemaPattern: Option[String]
    ): Either[String, Map[String, Table[?, ?, ?]]] =
      connection match
        case Some(conn) => inspectDatabase(conn, dialect, schemaPattern)
        case None => Left("@database specified but no database connection available")

    private def inspectDatabase(
      connection: Connection,
      dialect: Dialect,
      schemaPattern: Option[String]
    ): Either[String, Map[String, Table[?, ?, ?]]] =
      Try {
        val schemas = schemaPattern match
          case Some(pattern) => resolveSchemas(connection, pattern)
          case None => Seq.empty

        val tables = schemas.flatMap { schema =>
          dialect.inspectSchema(connection, schema)
        }
        tablesToMap(tables.map(_.asInstanceOf[Table[?, ?, ?]]))
      } match
        case Success(map) => Right(map)
        case Failure(e) => Left(s"Failed to inspect database schema: ${e.getMessage}")

    private def resolveSchemas(connection: Connection, pattern: String): Seq[String] =
      val md = connection.getMetaData
      val rs = md.getSchemas()
      val schemas = scala.collection.mutable.ArrayBuffer[String]()
      while rs.next() do
        val schemaName = rs.getString("TABLE_SCHEM")
        if schemaName != null then schemas += schemaName
      rs.close()

      val regex = pattern.replace("%", ".*").replace("_", ".").r
      schemas.filter(s => regex.matches(s)).toSeq

  /** Parse a schema source specification from CLI input.
    *
    * Syntax:
    *   - `@database` or `@db`: Use the current database connection
    *   - Any other string: Treat as a file pattern (glob supported)
    */
  def parse(value: String): SchemaSource =
    value.trim.toLowerCase match
      case "@database" | "@db" => CurrentDatabase
      case _ => Files(value)

  given SchemaSourceReader: TokensReader.Simple[SchemaSource] with
    def shortName = "schema-source"
    def read(strs: Seq[String]): Either[String, SchemaSource] =
      strs.headOption match
        case Some(value) => Right(parse(value))
        case None => Left("Missing schema source")

  given OptionSchemaSourceReader: TokensReader.Simple[Option[SchemaSource]] with
    def shortName = "schema-source"
    def read(strs: Seq[String]): Either[String, Option[SchemaSource]] =
      strs.headOption match
        case Some(value) => Right(Some(parse(value)))
        case None => Right(None)
