package sequala.parse

import sequala.common.{Dialect, DialectReader, Dialects}
import sequala.common.parser.ErrorMessage
import sequala.schema.codec.DialectCodecs
import fastparse.*
import scala.io.Source
import scala.collection.parallel.CollectionConverters.*
import java.io.{File, FileWriter, PrintWriter}
import java.nio.file.{FileSystems, Files, Paths}
import java.util.logging.{Level as JLevel, LogManager, Logger as JLogger}
import scribe.Logger
import scribe.Level
import io.circe.*
import io.circe.generic.semiauto.*
import io.circe.syntax.*
import io.circe.Printer
import io.circe.Encoder
import sequala.schema.Statement
import sequala.schema.SqlRenderer.toSql
import sequala.parse.Transforms
import sequala.parse.StatementTransform
import com.arakelian.jq.{ImmutableJqLibrary, ImmutableJqRequest}
import mainargs.{arg, main, Leftover, Parser, TokensReader}
import sequala.common.parser.SQLBaseObject
import sequala.common.renderer.ParserSqlRenderers
import io.circe.yaml.parser as yamlParser
import wvlet.log.LogSupport

case class BindDirective(varName: String, pathPattern: String, optional: Boolean)

sealed trait WriteDirective {
  def varName: String
  def pathPattern: String
}
case class SingleWriteDirective(varName: String, pathPattern: String) extends WriteDirective
case class MapWriteDirective(varName: String, pathPattern: String) extends WriteDirective

enum FormatResult:
  case MainOutput(content: String)
  case MultipleWrites(files: Map[String, String])

case class Annotation(file: String, line: Int, title: String, message: String, annotation_level: String)

object Annotation {
  implicit val encoder: Encoder[Annotation] = deriveEncoder[Annotation]
}

type OutputFormatFactory = Dialect => OutputFormat

given OutputFormatFactoryReader: TokensReader.Simple[OutputFormatFactory] with {
  def shortName = "format"
  def read(strs: Seq[String]): Either[String, OutputFormatFactory] = {
    if strs.isEmpty then {
      Left("Missing output format")
    } else {
      val s = strs.head
      s match {
        case "text" => Right(_ => TextFormat)
        case "sql" => Right(dialect => SqlFormat(dialect))
        case "json" => Right(dialect => StatementsJsonFormat(dialect))
        case s"jq-sql($query)" => Right(dialect => JqToSqlFormat(query, dialect))
        case s"jq-file-sql:$filePath" => Right(dialect => JqFileToSqlFormat(filePath, dialect))
        case s"jq($query)" => Right(dialect => JqFormat(query, dialect))
        case s"jq-file:$filePath" => Right(dialect => JqFileFormat(filePath, dialect))
        case _ =>
          Left(
            s"Unknown output format: $s. Valid formats: text, sql, json, jq(query), jq-file:path, jq-sql(query), jq-file-sql:path"
          )
      }
    }
  }
}

sealed trait OutputFormat {
  def name: String
  def format(result: ProcessingResult): FormatResult
  def withPretty(pretty: Boolean): OutputFormat = this
}

case object TextFormat extends OutputFormat {
  def name: String = "text"
  def format(result: ProcessingResult): FormatResult = {
    val textOutputLines = result.fileResults.flatMap { fileResult =>
      (s"\n=== Processing: ${fileResult.fileName}" :: fileResult.statementResults.flatMap { stmtResult =>
        val idx = stmtResult.index
        stmtResult.parseResult match {
          case Parsed.Success(_, _) =>
            List(s"✓ Statement ${idx + 1}: OK")
          case Parsed.Failure(label, index, _) =>
            val stmtIndex = index.toInt
            val absoluteLineNum = stmtResult.absoluteLineNumber.getOrElse(0)
            val context = if label == "unparseable" then {
              val fullText = stmtResult.context.statement
              if fullText.length <= 200 then fullText else fullText.take(200) + "..."
            } else {
              stmtResult.context.statement
                .substring(Math.max(0, stmtIndex - 50), Math.min(stmtResult.context.statement.length, stmtIndex + 50))
            }
            List(
              s"❌ ${stmtResult.context.relativePath}:${absoluteLineNum}: Syntax ERROR",
              s"Statement ${idx + 1} failed: $label",
              s"Context: $context"
            )
        }
      }) :+ fileResult.fileSummary
    }

    val overallSummary = s"\n=== Overall Summary ==="
    val summary2 = s"Files processed: ${result.fileResults.length}"
    val summary3 =
      s"Total: ${result.totalSuccessCount} successful, ${result.totalErrorCount} failed out of ${result.totalStatements} statements"

    FormatResult.MainOutput((textOutputLines :+ overallSummary :+ summary2 :+ summary3).mkString("\n"))
  }
}

case class SqlFormat(dialect: Dialect, pretty: Boolean = false) extends OutputFormat {
  import sequala.schema.{SqlFormatConfig, SqlRenderer}
  private val renderers = dialect.renderers
  import renderers.given

  def name: String = "sql"
  def format(result: ProcessingResult): FormatResult = {
    given SqlFormatConfig = if pretty then SqlFormatConfig.Pretty else SqlFormatConfig.Compact

    val sqlStatements = result.fileResults.flatMap { fileResult =>
      fileResult.statementResults.flatMap { stmtResult =>
        stmtResult.parseResult match {
          case Parsed.Success(stmt, _) => Some(SqlRenderer[Statement].toSql(stmt))
          case _ => None
        }
      }
    }

    FormatResult.MainOutput(sqlStatements.mkString("\n"))
  }
  override def withPretty(p: Boolean): OutputFormat = copy(pretty = p)
}

case class StatementsJsonFormat(dialect: Dialect) extends OutputFormat {
  private val codecs = dialect.codecs
  private given Encoder[Statement] = codecs.statementEncoder

  def name: String = "json"
  def format(result: ProcessingResult): FormatResult = {
    val statementsJson = result.asJson
    FormatResult.MainOutput(Printer.spaces2.copy(dropNullValues = true).print(statementsJson))
  }
}

case class JqFormat(query: String, dialect: Dialect) extends OutputFormat {
  private val jsonFormat = StatementsJsonFormat(dialect)
  private val helper = JqHelper(dialect)

  def name: String = "jq"
  def format(result: ProcessingResult): FormatResult = {
    val jsonContent = jsonFormat.format(result) match
      case FormatResult.MainOutput(content) => content
      case FormatResult.MultipleWrites(_) =>
        throw new IllegalStateException("JSON format should not produce multiple writes")
    FormatResult.MainOutput(JqHelperStatic.executeJq(jsonContent, query, ""))
  }
}

case class JqFileFormat(filePath: String, dialect: Dialect) extends OutputFormat {
  private val jsonFormat = StatementsJsonFormat(dialect)
  private val helper = JqHelper(dialect)

  def name: String = "jq-file"
  def format(result: ProcessingResult): FormatResult = {
    def getJsonContent(fr: FormatResult): String = fr match
      case FormatResult.MainOutput(content) => content
      case FormatResult.MultipleWrites(_) =>
        throw new IllegalStateException("JSON format should not produce multiple writes")

    val query = JqHelperStatic.readQueryFile(filePath)
    val bindDirectives = JqHelperStatic.parseBindDirectives(query)
    val writeDirectives = JqHelperStatic.parseWriteDirectives(query)

    if bindDirectives.isEmpty && writeDirectives.isEmpty then {
      FormatResult.MainOutput(
        JqHelperStatic.executeJq(getJsonContent(jsonFormat.format(result)), query, s"from file '$filePath'")
      )
    } else {
      val outputs = result.fileResults.map { fileResult =>
        val singleFileResult = ProcessingResult(
          fileResults = List(fileResult),
          totalSuccessCount = fileResult.statementResults.count(_.success),
          totalErrorCount = fileResult.statementResults.count(!_.success),
          totalStatements = fileResult.statementResults.length
        )
        val jsonStr = getJsonContent(jsonFormat.format(singleFileResult))
        val bindings = JqHelperStatic.resolveBindings(bindDirectives, fileResult.relativePath)
        val enrichedJson = JqHelperStatic.injectBindings(jsonStr, bindings)
        val jqOutput =
          JqHelperStatic.executeJq(enrichedJson, query, s"from file '$filePath' for '${fileResult.relativePath}'")
        (fileResult.relativePath, jqOutput)
      }

      if writeDirectives.isEmpty then {
        FormatResult.MainOutput(outputs.map(_._2).mkString("\n"))
      } else {
        val additionalWrites = outputs.flatMap { case (inputPath, jqOutput) =>
          helper.collectWrites(jqOutput, writeDirectives, inputPath)
        }.toMap
        FormatResult.MultipleWrites(additionalWrites)
      }
    }
  }
}

case class JqToSqlFormat(query: String, dialect: Dialect, pretty: Boolean = false) extends OutputFormat {
  private val jsonFormat = StatementsJsonFormat(dialect)
  private val helper = JqHelper(dialect)

  def name: String = "jq-sql"
  def format(result: ProcessingResult): FormatResult = {
    val jsonContent = jsonFormat.format(result) match
      case FormatResult.MainOutput(content) => content
      case FormatResult.MultipleWrites(_) =>
        throw new IllegalStateException("JSON format should not produce multiple writes")
    val jqOutput = JqHelperStatic.executeJq(jsonContent, query, "")
    FormatResult.MainOutput(helper.jsonToSql(jqOutput, pretty))
  }
  override def withPretty(p: Boolean): OutputFormat = copy(pretty = p)
}

case class JqFileToSqlFormat(filePath: String, dialect: Dialect, pretty: Boolean = false) extends OutputFormat {
  private val jsonFormat = StatementsJsonFormat(dialect)
  private val helper = JqHelper(dialect)

  def name: String = "jq-file-sql"
  def format(result: ProcessingResult): FormatResult = {
    def getJsonContent(fr: FormatResult): String = fr match
      case FormatResult.MainOutput(content) => content
      case FormatResult.MultipleWrites(_) =>
        throw new IllegalStateException("JSON format should not produce multiple writes")

    val query = JqHelperStatic.readQueryFile(filePath)
    val bindDirectives = JqHelperStatic.parseBindDirectives(query)
    val writeDirectives = JqHelperStatic.parseWriteDirectives(query)

    if bindDirectives.nonEmpty then {
      // Per-file processing: @bind directives need file-specific path info
      val outputs = result.fileResults.flatMap { fileResult =>
        val singleFileResult = ProcessingResult(
          fileResults = List(fileResult),
          totalSuccessCount = fileResult.statementResults.count(_.success),
          totalErrorCount = fileResult.statementResults.count(!_.success),
          totalStatements = fileResult.statementResults.length
        )
        val jsonStr = getJsonContent(jsonFormat.format(singleFileResult))
        val bindings = JqHelperStatic.resolveBindings(bindDirectives, fileResult.relativePath)
        val enrichedJson = JqHelperStatic.injectBindings(jsonStr, bindings)
        val jqOutput =
          JqHelperStatic.executeJq(enrichedJson, query, s"from file '$filePath' for '${fileResult.relativePath}'")
        Some((fileResult.relativePath, jqOutput))
      }

      if writeDirectives.isEmpty then {
        val sqlParts = outputs.flatMap { case (_, jqOutput) =>
          val sql = helper.jsonToSql(jqOutput, pretty)
          if sql.nonEmpty then Some(sql) else None
        }
        FormatResult.MainOutput(sqlParts.mkString("\n"))
      } else {
        val additionalWrites = outputs.flatMap { case (inputPath, jqOutput) =>
          helper.collectWrites(jqOutput, writeDirectives, inputPath, pretty)
        }.toMap
        FormatResult.MultipleWrites(additionalWrites)
      }
    } else {
      // Batch processing: process all files at once
      val jqOutput =
        JqHelperStatic.executeJq(getJsonContent(jsonFormat.format(result)), query, s"from file '$filePath'")

      if writeDirectives.isEmpty then {
        FormatResult.MainOutput(helper.jsonToSql(jqOutput, pretty))
      } else {
        // @write-map with batch processing: jq output contains all data, split by keys
        val additionalWrites = helper.collectWrites(jqOutput, writeDirectives, "", pretty)
        FormatResult.MultipleWrites(additionalWrites)
      }
    }
  }
  override def withPretty(p: Boolean): OutputFormat = copy(pretty = p)
}

class JqHelper(dialect: Dialect) extends LogSupport {
  private val codecs = dialect.codecs
  private val renderers = dialect.renderers
  private given Decoder[Statement] = codecs.statementDecoder

  def formatForExtension(json: Json, filePath: String, pretty: Boolean = false): String = {
    val ext = filePath.substring(filePath.lastIndexOf('.') + 1).toLowerCase
    debug(s"formatForExtension: filePath=$filePath, ext=$ext, pretty=$pretty")
    ext match {
      case "sql" => jsonToSql(Printer.noSpaces.print(json), pretty)
      case "yaml" | "yml" => JqHelperStatic.jsonToYaml(json)
      case "json" => Printer.spaces2.copy(dropNullValues = true).print(json)
      case _ => Printer.spaces2.copy(dropNullValues = true).print(json)
    }
  }

  def collectWrites(
    jqOutput: String,
    writeDirectives: Seq[WriteDirective],
    inputFilePath: String,
    pretty: Boolean = false
  ): Map[String, String] = {
    debug(s"collectWrites: ${writeDirectives.size} directives, pretty=$pretty")
    if writeDirectives.isEmpty then return Map.empty

    val json = io.circe.parser.parse(jqOutput) match {
      case Left(err) => throw new RuntimeException(s"Failed to parse jq output as JSON: ${err.message}")
      case Right(json) => json
    }

    writeDirectives.flatMap {
      case SingleWriteDirective(varName, pathPattern) =>
        val value = json.hcursor.downField(varName).focus.getOrElse {
          throw new RuntimeException(s"@write directive references missing key '$varName' in jq output")
        }
        val resolvedPath = JqHelperStatic.resolvePathPattern(pathPattern, inputFilePath)
        debug(s"collectWrites: SingleWrite varName=$varName -> $resolvedPath")
        Seq(resolvedPath -> formatForExtension(value, resolvedPath, pretty))

      case MapWriteDirective(varName, pathPattern) =>
        val obj = json.hcursor.downField(varName).focus.flatMap(_.asObject).getOrElse {
          throw new RuntimeException(s"@write-map directive requires '$varName' to be an object in jq output")
        }
        debug(s"collectWrites: MapWrite varName=$varName with ${obj.size} keys: ${obj.keys.mkString(", ")}")
        obj.toMap.map { case (key, value) =>
          val resolvedPath = JqHelperStatic.resolvePathPattern(pathPattern, inputFilePath, Some(key))
          // Check if statements in this value have sourceComment
          val stmtArray = value.asArray.getOrElse(Vector.empty)
          val stmtsWithComments = stmtArray.count { stmt =>
            stmt.hcursor.downField("table").downField("sourceComment").focus.exists(_.asArray.exists(_.nonEmpty))
          }
          debug(
            s"collectWrites: key=$key -> $resolvedPath, ${stmtArray.size} statements, $stmtsWithComments have table-level sourceComment"
          )
          resolvedPath -> formatForExtension(value, resolvedPath, pretty)
        }
    }.toMap
  }

  def jsonToSql(jsonString: String, pretty: Boolean = false): String = {
    import io.circe.parser.parse

    val trimmed = jsonString.trim
    if trimmed.isEmpty then return ""

    debug(s"jsonToSql: pretty=$pretty, input length=${trimmed.length}")

    parse(trimmed) match {
      case Left(parseError) =>
        throw new RuntimeException(s"Failed to parse jq output as JSON: ${parseError.message}")
      case Right(json) =>
        decodeStatements(json) match {
          case Left(error) =>
            throw new RuntimeException(s"Failed to decode JSON to Statement: $error")
          case Right(statements) =>
            import renderers.given
            import sequala.schema.{SqlFormatConfig, SqlRenderer, CreateTable}
            given config: SqlFormatConfig = if pretty then SqlFormatConfig.Pretty else SqlFormatConfig.Compact
            debug(s"jsonToSql: Decoded ${statements.size} statements, config.pretty=${config.pretty}")

            // Log sourceComment info for CreateTable statements
            statements.foreach {
              case ct: CreateTable[?, ?, ?] =>
                val tableComments = ct.table.sourceComment.size
                val colsWithComments = ct.table.columns.count(_.sourceComment.nonEmpty)
                debug(
                  s"jsonToSql: CreateTable '${ct.table.name}' has $tableComments table comments, $colsWithComments columns with comments"
                )
                if tableComments > 0 then
                  debug(s"jsonToSql:   Table comments: ${ct.table.sourceComment.map(_.text).mkString("; ")}")
                ct.table.columns.filter(_.sourceComment.nonEmpty).foreach { col =>
                  debug(s"jsonToSql:   Column '${col.name}' comments: ${col.sourceComment.map(_.text).mkString("; ")}")
                }
              case _ => ()
            }

            statements
              .map { stmt =>
                val sql = SqlRenderer[Statement].toSql(stmt)
                // Add semicolon if not already present
                if sql.trim.endsWith(";") then sql else sql + ";"
              }
              .mkString("\n")
        }
    }
  }

  def decodeStatements(json: Json): Either[String, Seq[Statement]] = {
    debug(s"decodeStatements: Attempting to decode JSON array")
    json.as[Seq[Statement]] match {
      case Right(statements) =>
        debug(s"decodeStatements: Successfully decoded ${statements.size} statements")
        Right(statements)
      case Left(seqError) =>
        // Debug: find which element fails
        json.asArray.foreach { arr =>
          debug(s"decodeStatements: Failed to decode as Seq, trying individual elements (${arr.size} total)")
          arr.zipWithIndex.foreach { case (elem, idx) =>
            elem.as[Statement] match {
              case Left(err) =>
                debug(s"decodeStatements: Statement $idx failed: ${err.message}")
                debug(s"decodeStatements: Statement $idx type was: ${elem.hcursor.get[String]("type")}")
              case Right(_) => ()
            }
          }
        }
        json.as[Statement] match {
          case Right(statement) =>
            debug(s"decodeStatements: Decoded as single statement")
            Right(Seq(statement))
          case Left(error) => Left(s"${error.message} at ${error.history.mkString(" -> ")}")
        }
    }
  }
}

object JqHelperStatic {
  private val bindDirectivePattern = """#\s*@bind\s+(\w+)\s+(.+?)(\s+optional)?\s*$""".r
  private val writeDirectivePattern = """#\s*@write\s+(\w+)\s+(.+?)\s*$""".r
  private val writeMapDirectivePattern = """#\s*@write-map\s+(\w+)\s+(.+?)\s*$""".r

  def suppressJqLoggers(): Unit = {
    val loggersToSuppress = Seq("com.arakelian.jq", "com.arakelian.jq.NativeLib", "com.arakelian.jq.JqLibrary")
    loggersToSuppress.foreach { loggerName =>
      val logger = JLogger.getLogger(loggerName)
      logger.setLevel(JLevel.OFF)
      logger.setUseParentHandlers(false)
      val handlers = logger.getHandlers
      handlers.foreach(logger.removeHandler)
    }
  }

  def parseBindDirectives(jqContent: String): Seq[BindDirective] = {
    jqContent.linesIterator.flatMap { line =>
      bindDirectivePattern.findFirstMatchIn(line).map { m =>
        BindDirective(varName = m.group(1), pathPattern = m.group(2).trim, optional = m.group(3) != null)
      }
    }.toSeq
  }

  def parseWriteDirectives(jqContent: String): Seq[WriteDirective] = {
    jqContent.linesIterator.flatMap { line =>
      writeMapDirectivePattern
        .findFirstMatchIn(line)
        .map { m =>
          MapWriteDirective(varName = m.group(1), pathPattern = m.group(2).trim)
        }
        .orElse {
          writeDirectivePattern.findFirstMatchIn(line).map { m =>
            SingleWriteDirective(varName = m.group(1), pathPattern = m.group(2).trim)
          }
        }
    }.toSeq
  }

  def resolvePathPattern(pattern: String, inputFilePath: String, key: Option[String] = None): String = {
    val inputFile = Paths.get(inputFilePath).toAbsolutePath.normalize
    val dir = inputFile.getParent.toString
    val fileName = inputFile.getFileName.toString
    val baseName = if fileName.contains(".") then fileName.substring(0, fileName.lastIndexOf('.')) else fileName

    var result = pattern
      .replace("{dir}", dir)
      .replace("{file}", baseName)
      .replace("{base}", fileName)

    key.foreach(k => result = result.replace("{key}", k))
    result
  }

  def resolveBindingPath(pattern: String, inputFilePath: String): String =
    resolvePathPattern(pattern, inputFilePath)

  def jsonToYaml(json: Json): String = {
    import io.circe.yaml.syntax.*
    json.asYaml.spaces2
  }

  def writeToFile(filePath: String, content: String): Unit = {
    val path = Paths.get(filePath)
    val parent = path.getParent
    if parent != null then Files.createDirectories(parent)
    val writer = new PrintWriter(new File(filePath))
    try {
      writer.print(content)
      if content.nonEmpty && !content.endsWith("\n") then writer.print("\n")
    } finally writer.close()
  }

  def loadYamlAsJson(filePath: String): Either[String, Json] = {
    val path = Paths.get(filePath)
    if !Files.exists(path) then {
      Left(s"File not found: $filePath")
    } else {
      val content = Source.fromFile(path.toFile).mkString
      yamlParser.parse(content) match {
        case Left(err) => Left(s"YAML parse error in $filePath: ${err.getMessage}")
        case Right(json) => Right(json)
      }
    }
  }

  def resolveBindings(directives: Seq[BindDirective], inputFilePath: String): Map[String, Json] = {
    directives.flatMap { directive =>
      val resolvedPath = resolveBindingPath(directive.pathPattern, inputFilePath)
      loadYamlAsJson(resolvedPath) match {
        case Right(json) => Some(directive.varName -> json)
        case Left(err) =>
          if !directive.optional then {
            throw new RuntimeException(s"Failed to load required binding '${directive.varName}': $err")
          }
          None
      }
    }.toMap
  }

  def injectBindings(jsonStr: String, bindings: Map[String, Json]): String = {
    if bindings.isEmpty then return jsonStr

    io.circe.parser.parse(jsonStr) match {
      case Left(err) => throw new RuntimeException(s"Failed to parse JSON for binding injection: ${err.message}")
      case Right(json) =>
        val bindingsJson = Json.obj(bindings.toSeq*)
        val enriched = json.deepMerge(Json.obj("bindings" -> bindingsJson))
        Printer.spaces2.copy(dropNullValues = true).print(enriched)
    }
  }

  def readQueryFile(filePath: String): String = {
    try {
      val currentDir = Paths.get(".").toAbsolutePath.normalize
      val queryFile = if Paths.get(filePath).isAbsolute then {
        Paths.get(filePath).toFile
      } else {
        currentDir.resolve(filePath).toFile
      }

      if !queryFile.exists() || !queryFile.isFile then {
        throw new RuntimeException(s"JQ query file not found: $filePath")
      }

      Source.fromFile(queryFile).mkString.trim
    } catch {
      case e: RuntimeException => throw e
      case e: Exception =>
        throw new RuntimeException(s"Error reading JQ query file '$filePath': ${e.getMessage}", e)
    }
  }

  def executeJq(input: String, query: String, context: String): String = {
    try {
      suppressJqLoggers()
      val library = ImmutableJqLibrary.of()
      val request = ImmutableJqRequest
        .builder()
        .lib(library)
        .input(input)
        .filter(query)
        .build()
      request.execute().getOutput
    } catch {
      case e: Exception =>
        val ctxMsg = if context.nonEmpty then s" $context" else ""
        throw new RuntimeException(s"Error executing JQ query$ctxMsg: ${e.getMessage}", e)
    }
  }
}

sealed trait OutputDestination {
  def write(content: String, path: Option[String] = None): Unit
}

case object ConsoleDestination extends OutputDestination {
  def write(content: String, path: Option[String] = None): Unit = {
    path.foreach(p => println(s"=== $p ==="))
    println(content)
  }
}

case class FileDestination(defaultPath: String) extends OutputDestination {
  def write(content: String, path: Option[String] = None): Unit = {
    val targetPath = path.getOrElse(defaultPath)
    JqHelperStatic.writeToFile(targetPath, content)
  }
}

case class DirectoryDestination(baseDir: String) extends OutputDestination {
  def write(content: String, path: Option[String] = None): Unit = path match {
    case Some(p) =>
      val fullPath = Paths.get(baseDir).resolve(p).toString
      JqHelperStatic.writeToFile(fullPath, content)
    case None => throw new RuntimeException("DirectoryDestination requires a path for each write")
  }
}

object OutputDestination {
  implicit val tokensReader: TokensReader.Simple[OutputDestination] = new TokensReader.Simple[OutputDestination] {
    def shortName = "destination"
    def read(strs: Seq[String]): Either[String, OutputDestination] = {
      if strs.isEmpty then {
        Left("Missing output destination")
      } else {
        strs.head match {
          case "console" => Right(ConsoleDestination)
          case path if path.endsWith("/") => Right(DirectoryDestination(path))
          case path => Right(FileDestination(path))
        }
      }
    }
  }
}

case class StatementContext(
  statement: String,
  rawStatement: String,
  relativePath: String,
  dialectName: String,
  statementStartInContent: Int
)

object StatementContext {
  implicit val encoder: Encoder[StatementContext] = deriveEncoder[StatementContext]
}

case class StatementResult(
  index: Int,
  parseResult: Parsed[Statement],
  context: StatementContext,
  absoluteLineNumber: Option[Int] = None
) {
  def success: Boolean = parseResult match {
    case Parsed.Success(_, _) => true
    case _: Parsed.Failure => false
  }
}

object ParsedEncoder {
  given parsedStatementEncoder(using stmtEncoder: Encoder[Statement]): Encoder[Parsed[Statement]] = Encoder.instance {
    case Parsed.Success(value, _) =>
      Json.obj("success" -> Json.fromBoolean(true), "value" -> stmtEncoder(value))
    case Parsed.Failure(label, index, extra) =>
      Json.obj("success" -> Json.fromBoolean(false), "label" -> Json.fromString(label), "index" -> Json.fromLong(index))
  }
}

object StatementResult {
  import ParsedEncoder.given
  given (using stmtEncoder: Encoder[Statement]): Encoder[StatementResult] = Encoder.instance { sr =>
    Json.obj(
      "index" -> Json.fromInt(sr.index),
      "parseResult" -> sr.parseResult.asJson,
      "context" -> sr.context.asJson,
      "absoluteLineNumber" -> sr.absoluteLineNumber.map(Json.fromInt).getOrElse(Json.Null)
    )
  }
}

case class FileResult(
  fileName: String,
  relativePath: String,
  statementResults: List[StatementResult],
  fileSummary: String
)

object FileResult {
  import StatementResult.given
  given (using Encoder[Statement]): Encoder[FileResult] = Encoder.instance { fr =>
    Json.obj(
      "fileName" -> Json.fromString(fr.fileName),
      "relativePath" -> Json.fromString(fr.relativePath),
      "statementResults" -> fr.statementResults.asJson,
      "fileSummary" -> Json.fromString(fr.fileSummary)
    )
  }
}

case class ProcessingResult(
  fileResults: List[FileResult],
  totalSuccessCount: Int,
  totalErrorCount: Int,
  totalStatements: Int
)

object ProcessingResult {
  import FileResult.given
  given (using Encoder[Statement]): Encoder[ProcessingResult] = Encoder.instance { pr =>
    Json.obj(
      "fileResults" -> pr.fileResults.asJson,
      "totalSuccessCount" -> Json.fromInt(pr.totalSuccessCount),
      "totalErrorCount" -> Json.fromInt(pr.totalErrorCount),
      "totalStatements" -> Json.fromInt(pr.totalStatements)
    )
  }
}

object ParseRunner {
  private val logger = Logger("sequala.parse.ParseRunner")

  implicit val dialectReader: TokensReader.Simple[Dialect] = DialectReader
  implicit val outputFormatFactoryReader: TokensReader.Simple[OutputFormatFactory] = OutputFormatFactoryReader

  def run(
    dialect: Dialect,
    outputFactory: OutputFormatFactory,
    writeTo: OutputDestination,
    pretty: Boolean,
    transformTokens: Seq[String],
    filePatterns: Seq[String],
    withComments: Boolean = false
  ): Unit = {
    val output = outputFactory(dialect)
    val shouldSuppressLogs = writeTo == ConsoleDestination
    val logLevel = if shouldSuppressLogs then {
      Option(System.getProperty("log.level"))
        .orElse(Option(System.getenv("LOG_LEVEL")))
        .getOrElse("WARN")
    } else {
      Option(System.getProperty("log.level"))
        .orElse(Option(System.getenv("LOG_LEVEL")))
        .getOrElse("DEBUG")
    }

    val level = logLevel.toUpperCase match {
      case "TRACE" => Level.Trace
      case "DEBUG" => Level.Debug
      case "INFO" => Level.Info
      case "WARN" => Level.Warn
      case "ERROR" => Level.Error
      case _ => Level.Debug
    }

    Logger.root
      .clearHandlers()
      .withHandler(minimumLevel = Some(level))
      .replace()

    if shouldSuppressLogs then {
      val loggersToSuppress = Seq("com.arakelian.jq", "com.arakelian.jq.NativeLib", "com.arakelian.jq.JqLibrary")

      loggersToSuppress.foreach { loggerName =>
        val jlogger = JLogger.getLogger(loggerName)
        jlogger.setLevel(JLevel.OFF)
        jlogger.setUseParentHandlers(false)
      }
    }

    val files = filePatterns.flatMap { pattern =>
      val currentDir = Paths.get(".").toAbsolutePath.normalize

      val directFile = if Paths.get(pattern).isAbsolute then {
        Paths.get(pattern).toFile
      } else {
        currentDir.resolve(pattern).toFile
      }

      if directFile.exists() && directFile.isFile && !pattern.contains('*') && !pattern.contains('?') then {
        Seq(directFile)
      } else if pattern.contains('*') || pattern.contains('?') then {
        val normalizedPattern = pattern.replace('\\', '/')

        val parts = normalizedPattern.split("/").filter(_.nonEmpty)
        def findBaseRec(path: java.nio.file.Path, index: Int): (java.nio.file.Path, Int) =
          if index >= parts.length then {
            (path, index)
          } else {
            val part = parts(index)
            if part == ".." then {
              findBaseRec(path.getParent, index + 1)
            } else if part == "." then {
              findBaseRec(path, index + 1)
            } else if part.contains('*') || part.contains('?') then {
              (path, index)
            } else {
              val nextPath = path.resolve(part)
              if nextPath.toFile.exists() && nextPath.toFile.isDirectory then {
                findBaseRec(nextPath, index + 1)
              } else {
                (path, index)
              }
            }
          }

        val (baseDir, baseIndex) = findBaseRec(currentDir, 0)

        val relativePattern = if baseIndex < parts.length then {
          parts.slice(baseIndex, parts.length).mkString("/")
        } else {
          "**"
        }

        val basePath = baseDir.normalize

        if basePath.toFile.exists() && basePath.toFile.isDirectory then {
          val fs = FileSystems.getDefault
          val matcher = fs.getPathMatcher(s"glob:$relativePattern")

          try {
            import scala.jdk.CollectionConverters.*
            Files
              .walk(basePath)
              .iterator()
              .asScala
              .filter(_.toFile.isFile)
              .filter { path =>
                val relativePath = basePath.relativize(path).toString.replace('\\', '/')
                matcher.matches(Paths.get(relativePath))
              }
              .map(_.toFile)
              .toSeq
          } catch {
            case _: Exception =>
              Seq.empty
          }
        } else {
          Seq.empty
        }
      } else {
        Seq.empty
      }
    }.distinct

    if files.isEmpty then {
      logger.warn(s"No files found matching: ${filePatterns.mkString(", ")}")
      System.exit(1)
    }

    val result = parseFiles(files, dialect, withComments)

    // Apply transforms if specified
    val transformedResult =
      if transformTokens.nonEmpty then
        Transforms.resolveTransforms(transformTokens, dialect) match
          case Right(transforms) =>
            applyTransforms(result, transforms)
          case Left(err) =>
            System.err.println(s"Error resolving transforms: $err")
            sys.exit(1)
      else result

    val outputWithPretty = output.withPretty(pretty)
    generateOutputs(transformedResult, outputWithPretty, writeTo)

    if result.totalErrorCount > 0 then {
      System.exit(1)
    }
  }

  def parseFiles(files: Seq[File], dialect: Dialect, withComments: Boolean = false): ProcessingResult = {
    val fileResults = files.par.map { file =>
      val relativePath = {
        val currentDir = Paths.get(".").toAbsolutePath.normalize
        val filePathObj = file.toPath.toAbsolutePath.normalize
        try
          currentDir.relativize(filePathObj).toString.replace('\\', '/')
        catch {
          case _: Exception => file.getPath
        }
      }

      val content = Source.fromFile(file).mkString

      val dialectName = dialect.name

      // Use parseAllWithComments for Oracle when withComments is enabled
      val parseResults: Seq[sequala.common.statement.StatementParseResult] =
        if withComments && dialect.name == "oracle" then sequala.oracle.OracleSQL.parseAllWithComments(content)
        else dialect.parser.parseAll(content)

      val statementResults = parseResults.zipWithIndex.map { case (parseResult, idx) =>
        val (statementText, isUnparseable) = parseResult.result match {
          case Right(stmt) => (stmt.toString, false)
          case Left(unparseable) => (unparseable.content, true)
        }

        val absoluteLineNum = if parseResult.startPos >= 0 && parseResult.startPos < content.length then {
          val contentBeforeStmt = content.substring(0, parseResult.startPos)
          val linesBeforeStmt = contentBeforeStmt.count(_ == '\n')
          Some(linesBeforeStmt + 1)
        } else {
          None
        }

        val finalParseResult = parseResult.result match {
          case Right(stmt) => Parsed.Success(stmt, 0)
          case Left(_) => Parsed.Failure("unparseable", 0, null)
        }

        val stmtContext = StatementContext(
          statement = statementText,
          rawStatement = statementText,
          relativePath = relativePath,
          dialectName = dialectName,
          statementStartInContent = parseResult.startPos
        )

        val stmtResult = StatementResult(
          index = idx,
          parseResult = finalParseResult,
          context = stmtContext,
          absoluteLineNumber = absoluteLineNum
        )

        parseResult.result match {
          case Left(unparseable) =>
            val stmtLines = statementText.split("\n")
            logger.info(s"  Label: unparseable")
            logger.info(s"  Context: ${statementText.take(100)}")
            logger.debug(s"  Full statement:")
            logger.debug(s"  ${stmtLines.mkString("\n  ")}")
          case Right(_) =>
        }

        stmtResult
      }.toList

      val successCount = statementResults.count(_.success)
      val errorCount = statementResults.count(!_.success)
      val summary =
        s"File summary: $successCount successful, $errorCount failed out of ${statementResults.length} statements"

      FileResult(
        fileName = file.getName,
        relativePath = relativePath,
        statementResults = statementResults,
        fileSummary = summary
      )
    }.toList

    val totals = fileResults.foldLeft((0, 0, 0)) { case ((success, error, total), fileResult) =>
      val stmtCounts = fileResult.statementResults.foldLeft((0, 0)) { case ((s, e), stmt) =>
        if stmt.success then (s + 1, e) else (s, e + 1)
      }
      (success + stmtCounts._1, error + stmtCounts._2, total + fileResult.statementResults.length)
    }

    ProcessingResult(
      fileResults = fileResults,
      totalSuccessCount = totals._1,
      totalErrorCount = totals._2,
      totalStatements = totals._3
    )
  }

  def applyTransforms(result: ProcessingResult, transforms: Seq[StatementTransform]): ProcessingResult = {
    // Extract all successfully parsed statements
    val successfulStatements = result.fileResults.flatMap { fileResult =>
      fileResult.statementResults.flatMap { stmtResult =>
        stmtResult.parseResult match
          case Parsed.Success(stmt, _) => Some(stmt)
          case _ => None
      }
    }

    // Apply transforms
    val transformedStatements = Transforms.apply(successfulStatements, transforms)

    // Rebuild fileResults with transformed statements
    // We'll create a single "virtual" file result containing all transformed statements
    val transformedFileResults = if transformedStatements.nonEmpty then
      val transformedStatementResults = transformedStatements.zipWithIndex.map { case (stmt, idx) =>
        StatementResult(
          index = idx,
          parseResult = Parsed.Success(stmt, 0),
          context = StatementContext(
            statement = stmt.toString,
            rawStatement = stmt.toString,
            relativePath = "transformed",
            dialectName = result.fileResults.headOption
              .map(_.statementResults.headOption.map(_.context.dialectName).getOrElse("unknown"))
              .getOrElse("unknown"),
            statementStartInContent = 0
          ),
          absoluteLineNumber = None
        )
      }.toList

      List(
        FileResult(
          fileName = "transformed",
          relativePath = "transformed",
          statementResults = transformedStatementResults,
          fileSummary = s"File summary: ${transformedStatements.length} transformed statements"
        )
      )
    else List.empty

    ProcessingResult(
      fileResults = transformedFileResults,
      totalSuccessCount = transformedStatements.length,
      totalErrorCount = 0,
      totalStatements = transformedStatements.length
    )
  }

  def generateOutputs(result: ProcessingResult, output: OutputFormat, writeTo: OutputDestination): Unit = {
    output.format(result) match {
      case FormatResult.MainOutput(content) =>
        if content.nonEmpty then writeTo.write(content)
      case FormatResult.MultipleWrites(files) =>
        files.foreach { case (path, content) =>
          writeTo.write(content, Some(path))
        }
    }
  }
}

object RenderRunner {
  def run(dialect: Dialect, inputFile: String, pretty: Boolean, writeTo: OutputDestination): Unit = {
    import io.circe.parser.parse
    import sequala.schema.{SqlFormatConfig, SqlRenderer}

    val jsonString = Source.fromFile(inputFile).mkString
    val codecs = dialect.codecs
    val renderers = dialect.renderers
    given Decoder[Statement] = codecs.statementDecoder

    parse(jsonString) match {
      case Left(parseError) =>
        System.err.println(s"Failed to parse JSON: ${parseError.message}")
        sys.exit(1)
      case Right(json) =>
        json.as[Seq[Statement]] match {
          case Left(decodeError) =>
            json.asArray.foreach { arr =>
              arr.zipWithIndex.foreach { case (elem, idx) =>
                elem.as[Statement] match {
                  case Left(err) =>
                    System.err.println(s"Statement $idx failed: ${err.message}")
                    System.err.println(s"  type: ${elem.hcursor.get[String]("type")}")
                  case _ => ()
                }
              }
            }
            System.err.println(s"Failed to decode statements: ${decodeError.message}")
            sys.exit(1)
          case Right(statements) =>
            import renderers.given
            given SqlFormatConfig = if pretty then SqlFormatConfig.Pretty else SqlFormatConfig.Compact
            val sql = statements.map(stmt => SqlRenderer[Statement].toSql(stmt)).mkString("\n")
            writeTo.write(sql)
        }
    }
  }
}
