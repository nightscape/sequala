package sequala.migrate.cli

import io.circe.Encoder
import io.circe.generic.semiauto.*
import io.circe.syntax.*
import sequala.common.{Dialect, FilePatternResolver, OracleDialect}
import sequala.migrate.{DataDiffOp, DataDiffOptions, DataDiffer, DeleteRow, InsertRow, UpdateRow}
import sequala.parse.OutputDestination
import sequala.schema.{Insert, SqlFormatConfig, SqlRenderer, Statement, Table}
import sequala.schema.ast.{Expression, Name}
import sequala.DbConfig

import java.io.File
import java.sql.{Connection, DriverManager}
import scala.io.Source
import scala.util.{Failure, Success, Try}

object DataDiffRunner:

  def runDiff(
    from: String,
    to: String,
    dialect: Dialect,
    keyColumns: Option[String],
    ddlFile: Option[String],
    format: String,
    pretty: Boolean,
    writeTo: OutputDestination,
    withDeletes: Boolean
  ): Unit =
    val result = for
      fromStatements <- parseDataFile(from, dialect)
      toStatements <- parseDataFile(to, dialect)
      ddlTables <- ddlFile.map(parseDdlFile(_, dialect)).getOrElse(Right(Map.empty))
      options = DataDiffOptions(
        keyColumns = keyColumns.map(_.split(",").toSeq.map(_.trim)),
        generateDeletes = withDeletes
      )
      fromData = DataDiffer.parseInserts(fromStatements)
      toData = DataDiffer.parseInserts(toStatements)
      diffs <- DataDiffer.diff(fromData, toData, options, ddlTables)
    yield diffs

    result match
      case Left(error) =>
        System.err.println(s"Error: $error")
        System.exit(1)
      case Right(diffs) =>
        if diffs.isEmpty then writeTo.write("-- No data differences found")
        else
          val output = formatOutput(diffs, format, pretty, dialect)
          writeTo.write(output)

  def runPlan(
    source: String,
    db: DbConfig,
    keyColumns: Option[String],
    ddlFile: Option[String],
    tableFilter: Option[String],
    format: String,
    pretty: Boolean,
    writeTo: OutputDestination,
    withDeletes: Boolean
  ): Unit =
    val configResult = for
      config <- ConfigLoader.loadConfig(db.config.map(_.toString), db.env)
      dbConfig <- ConfigLoader.loadDatabaseConfig(
        config,
        urlOverride = db.database,
        userOverride = db.user,
        passwordOverride = db.password,
        schemaOverride = db.schema,
        dialectOverride = db.dialect
      )
    yield dbConfig

    configResult match
      case Left(error) =>
        System.err.println(s"Error: $error")
        System.exit(1)
      case Right(dbConfig) =>
        val dialect = dbConfig.inferredDialect
        val dialectName = dialect.name

        createConnection(dbConfig) match
          case Left(error) =>
            System.err.println(s"Error: $error")
            System.exit(1)
          case Right(connection) =>
            try
              val schemaName = dbConfig.schema

              val result = for
                toStatements <- parseDataFile(source, dialect)
                ddlTables <- ddlFile.map(parseDdlFile(_, dialect)).getOrElse(Right(Map.empty))
                options = DataDiffOptions(
                  keyColumns = keyColumns.map(_.split(",").toSeq.map(_.trim)),
                  generateDeletes = withDeletes
                )
                toData = DataDiffer.parseInserts(toStatements)
                tableNames = toData.keys.toSeq
                fromStatements = tableNames.flatMap { tableName =>
                  val parts = tableName.split("\\.")
                  val (schema, table) = if parts.length > 1 then (parts(0), parts(1)) else (schemaName, parts(0))
                  Try(DataExporter.exportAsInserts(connection, schema, table, dialectName)).toOption
                    .getOrElse(Seq.empty)
                }
                fromData = DataDiffer.parseInserts(fromStatements)
                diffs <- DataDiffer.diff(fromData, toData, options, ddlTables)
              yield diffs

              result match
                case Left(error) =>
                  System.err.println(s"Error: $error")
                  System.exit(1)
                case Right(diffs) =>
                  if diffs.isEmpty then writeTo.write("-- No data differences found")
                  else
                    val output = formatOutput(diffs, format, pretty, dialect)
                    writeTo.write(output)
            finally connection.close()

  private def createConnection(dbConfig: DatabaseConfig): Either[String, Connection] =
    if dbConfig.inferredDialect == OracleDialect then Try(Class.forName("oracle.jdbc.OracleDriver"))

    Try {
      val props = new java.util.Properties()
      dbConfig.user.foreach(props.setProperty("user", _))
      dbConfig.password.foreach(props.setProperty("password", _))
      DriverManager.getConnection(dbConfig.jdbcUrl, props)
    } match
      case Success(conn) => Right(conn)
      case Failure(e) => Left(s"Failed to connect to database: ${e.getMessage}")

  private def parseDataFile(path: String, dialect: Dialect): Either[String, Seq[Statement]] =
    val files = FilePatternResolver.resolve(path)
    if files.isEmpty then Left(s"No files match pattern: $path")
    else
      val results = files.flatMap { file =>
        Try(Source.fromFile(file).mkString) match
          case Failure(e) =>
            System.err.println(s"Warning: Failed to read ${file.getName}: ${e.getMessage}")
            Seq.empty
          case Success(content) =>
            val parseResults = dialect.parser.parseAll(content)
            parseResults.flatMap(_.result.toOption)
      }
      Right(results)

  private def parseDdlFile(path: String, dialect: Dialect): Either[String, Map[String, Table[?, ?, ?]]] =
    val file = new File(path)
    if !file.exists() then Left(s"DDL file not found: $path")
    else
      Try(Source.fromFile(file).mkString) match
        case Failure(e) => Left(s"Failed to read DDL file: ${e.getMessage}")
        case Success(content) =>
          dialect.parseSourceDDL(content) match
            case Left(err) => Left(s"Failed to parse DDL: $err")
            case Right(tables) =>
              val byName = tables.map(t => t.name.toUpperCase -> t).toMap ++
                tables.flatMap(t => t.schema.map(s => s"${s.toUpperCase}.${t.name.toUpperCase}" -> t)).toMap
              Right(byName)

  private def formatOutput(diffs: Seq[DataDiffOp], format: String, pretty: Boolean, dialect: Dialect): String =
    given SqlFormatConfig = if pretty then SqlFormatConfig.Pretty else SqlFormatConfig.Compact
    val renderers = dialect.renderers
    import renderers.given

    def renderName(n: Name): String = summon[SqlRenderer[Name]].toSql(n)
    def renderExpr(e: Expression): String = summon[SqlRenderer[Expression]].toSql(e)

    format.toLowerCase match
      case "json" =>
        case class DataDiffJson(operation: String, table: String, sql: String)
        given Encoder[DataDiffJson] = deriveEncoder

        val jsonDiffs = diffs.map { diff =>
          val (op, table, sql) = diff match
            case InsertRow(t, cols, vals) =>
              val colsStr = cols.map(renderName).mkString(", ")
              val valsStr = vals.map(renderExpr).mkString(", ")
              ("INSERT", t.name, s"INSERT INTO ${renderName(t)} ($colsStr) VALUES ($valsStr)")
            case UpdateRow(t, keyCols, setCols) =>
              val setStr = setCols.map { case (n, e) => s"${renderName(n)} = ${renderExpr(e)}" }.mkString(", ")
              val whereStr = keyCols.map { case (n, e) => s"${renderName(n)} = ${renderExpr(e)}" }.mkString(" AND ")
              ("UPDATE", t.name, s"UPDATE ${renderName(t)} SET $setStr WHERE $whereStr")
            case DeleteRow(t, keyCols) =>
              val whereStr = keyCols.map { case (n, e) => s"${renderName(n)} = ${renderExpr(e)}" }.mkString(" AND ")
              ("DELETE", t.name, s"DELETE FROM ${renderName(t)} WHERE $whereStr")
          DataDiffJson(op, table, sql)
        }
        jsonDiffs.asJson.spaces2

      case _ => // sql or text
        val output = new StringBuilder
        diffs.foreach { diff =>
          val sql = diff match
            case InsertRow(t, cols, vals) =>
              val colsStr = cols.map(renderName).mkString(", ")
              val valsStr = vals.map(renderExpr).mkString(", ")
              s"INSERT INTO ${renderName(t)} ($colsStr) VALUES ($valsStr)"
            case UpdateRow(t, keyCols, setCols) =>
              val setStr = setCols.map { case (n, e) => s"${renderName(n)} = ${renderExpr(e)}" }.mkString(", ")
              val whereStr = keyCols.map { case (n, e) => s"${renderName(n)} = ${renderExpr(e)}" }.mkString(" AND ")
              s"UPDATE ${renderName(t)} SET $setStr WHERE $whereStr"
            case DeleteRow(t, keyCols) =>
              val whereStr = keyCols.map { case (n, e) => s"${renderName(n)} = ${renderExpr(e)}" }.mkString(" AND ")
              s"DELETE FROM ${renderName(t)} WHERE $whereStr"
          output.append(sql).append(";\n")
        }
        output.toString.stripSuffix("\n")
