package sequala.parse

import sequala.oracle.OracleSQL
import sequala.ansi.ANSISQL
import sequala.postgres.PostgresSQL
import sequala.common.parser.ErrorMessage
import sequala.common.codec.CirceCodecs._
import fastparse._
import scala.io.Source
import java.io.{File, FileWriter, PrintWriter}
import java.nio.file.{FileSystems, Files, Paths}
import java.util.logging.{Level => JLevel, LogManager, Logger => JLogger}
import scribe.Logger
import scribe.Level
import io.circe._
import io.circe.generic.semiauto._
import io.circe.syntax._
import io.circe.Printer
import io.circe.Encoder
import sequala.common.statement.Statement
import com.arakelian.jq.{ImmutableJqLibrary, ImmutableJqRequest}
import mainargs.{arg, main, Leftover, Parser, TokensReader}
import sequala.common.parser.SQLBaseObject

case class Annotation(file: String, line: Int, title: String, message: String, annotation_level: String)

object Annotation {
  implicit val encoder: Encoder[Annotation] = deriveEncoder[Annotation]
}

object SQLDialectMaps {
  val dialects: Seq[SQLBaseObject] = Seq(OracleSQL, ANSISQL, PostgresSQL)
  val dialectNames: Seq[String] = dialects.map(_.name)
  val nameToDialect: Map[String, SQLBaseObject] = dialects.map(d => d.name -> d).toMap
  val validDialects: String = dialects.map(_.name).mkString(", ")
}

given SQLBaseObjectReader: TokensReader.Simple[SQLBaseObject] with {
  def shortName = "dialect"
  def read(strs: Seq[String]): Either[String, SQLBaseObject] = {
    if (strs.isEmpty) {
      Left("Missing dialect name")
    } else {
      SQLDialectMaps.nameToDialect.get(strs.head.toLowerCase) match {
        case Some(dialect) => Right(dialect)
        case None => Left(s"Unknown dialect: ${strs.head}. Valid dialects are: ${SQLDialectMaps.validDialects}")
      }
    }
  }
}

given OutputFormatReader: TokensReader.Simple[OutputFormat] with {
  def shortName = "format"
  def read(strs: Seq[String]): Either[String, OutputFormat] = {
    if (strs.isEmpty) {
      Left("Missing output format")
    } else {
      val s = strs.head
      s match {
        case "text" => Right(TextFormat)
        case "json" => Right(StatementsJsonFormat)
        case s"jq($query)" => Right(JqFormat(query))
        case s"jq-file:$filePath" => Right(JqFileFormat(filePath))
        case _ => Left(s"Unknown output format: $s. Valid formats: text, json, jq(query), jq-file:path")
      }
    }
  }
}

sealed trait OutputFormat {
  def name: String
  def format(result: ProcessingResult): String
}

case object TextFormat extends OutputFormat {
  def name: String = "text"
  def format(result: ProcessingResult): String = {
    val textOutputLines = result.fileResults.flatMap { fileResult =>
      (s"\n=== Processing: ${fileResult.fileName}" :: fileResult.statementResults.flatMap { stmtResult =>
        val idx = stmtResult.index
        stmtResult.parseResult match {
          case Parsed.Success(_, _) =>
            List(s"✓ Statement ${idx + 1}: OK")
          case Parsed.Failure(label, index, _) =>
            val stmtIndex = index.toInt
            val absoluteLineNum = stmtResult.absoluteLineNumber.getOrElse(0)
            val context = if (label == "unparseable") {
              val fullText = stmtResult.context.statement
              if (fullText.length <= 200) fullText else fullText.take(200) + "..."
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

    (textOutputLines :+ overallSummary :+ summary2 :+ summary3).mkString("\n")
  }
}

case object StatementsJsonFormat extends OutputFormat {
  import ParsedEncoder._
  def name: String = "json"
  def format(result: ProcessingResult): String = {
    val statementsJson = result.asJson
    Printer.spaces2.copy(dropNullValues = true).print(statementsJson)
  }
}

case class JqFormat(query: String) extends OutputFormat {
  def name: String = "jq"
  def format(result: ProcessingResult): String = {
    val statementsJsonString = StatementsJsonFormat.format(result)
    try {
      val loggersToSuppress = Seq("com.arakelian.jq", "com.arakelian.jq.NativeLib", "com.arakelian.jq.JqLibrary")

      loggersToSuppress.foreach { loggerName =>
        val logger = JLogger.getLogger(loggerName)
        logger.setLevel(JLevel.OFF)
        logger.setUseParentHandlers(false)
        val handlers = logger.getHandlers
        handlers.foreach(logger.removeHandler)
      }

      val library = ImmutableJqLibrary.of()
      val request = ImmutableJqRequest
        .builder()
        .lib(library)
        .input(statementsJsonString)
        .filter(query)
        .build()
      request.execute().getOutput
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Error executing JQ query: ${e.getMessage}", e)
    }
  }
}

case class JqFileFormat(filePath: String) extends OutputFormat {
  def name: String = "jq-file"
  def format(result: ProcessingResult): String = {
    val query =
      try {
        val currentDir = Paths.get(".").toAbsolutePath.normalize
        val queryFile = if (Paths.get(filePath).isAbsolute) {
          Paths.get(filePath).toFile
        } else {
          currentDir.resolve(filePath).toFile
        }

        if (!queryFile.exists() || !queryFile.isFile) {
          throw new RuntimeException(s"JQ query file not found: $filePath")
        }

        Source.fromFile(queryFile).mkString.trim
      } catch {
        case e: RuntimeException => throw e
        case e: Exception =>
          throw new RuntimeException(s"Error reading JQ query file '$filePath': ${e.getMessage}", e)
      }

    val statementsJsonString = StatementsJsonFormat.format(result)
    try {
      val loggersToSuppress = Seq("com.arakelian.jq", "com.arakelian.jq.NativeLib", "com.arakelian.jq.JqLibrary")

      loggersToSuppress.foreach { loggerName =>
        val logger = JLogger.getLogger(loggerName)
        logger.setLevel(JLevel.OFF)
        logger.setUseParentHandlers(false)
        val handlers = logger.getHandlers
        handlers.foreach(logger.removeHandler)
      }

      val library = ImmutableJqLibrary.of()
      val request = ImmutableJqRequest
        .builder()
        .lib(library)
        .input(statementsJsonString)
        .filter(query)
        .build()
      request.execute().getOutput
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Error executing JQ query from file '$filePath': ${e.getMessage}", e)
    }
  }
}

sealed trait OutputDestination {
  def write(content: String): Unit
}

case object ConsoleDestination extends OutputDestination {
  def write(content: String): Unit = println(content)
}

case class FileDestination(path: String) extends OutputDestination {
  def write(content: String): Unit = {
    val writer = new PrintWriter(new File(path))
    try
      writer.println(content)
    finally
      writer.close()
  }
}

object OutputDestination {
  implicit val tokensReader: TokensReader.Simple[OutputDestination] = new TokensReader.Simple[OutputDestination] {
    def shortName = "destination"
    def read(strs: Seq[String]): Either[String, OutputDestination] = {
      if (strs.isEmpty) {
        Left("Missing output destination")
      } else {
        strs.head match {
          case "console" => Right(ConsoleDestination)
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
  implicit def parsedEncoder[T : Encoder]: Encoder[Parsed[T]] = Encoder.instance {
    case Parsed.Success(value, _) =>
      Json.obj("success" -> Json.fromBoolean(true), "value" -> value.asJson)
    case Parsed.Failure(label, index, extra) =>
      Json.obj("success" -> Json.fromBoolean(false), "label" -> Json.fromString(label), "index" -> Json.fromLong(index))
  }
}

object StatementResult {
  import ParsedEncoder._
  implicit val encoder: Encoder[StatementResult] = deriveEncoder[StatementResult]
}

case class FileResult(
  fileName: String,
  relativePath: String,
  statementResults: List[StatementResult],
  fileSummary: String
)

object FileResult {
  implicit val encoder: Encoder[FileResult] = deriveEncoder[FileResult]
}

case class ProcessingResult(
  fileResults: List[FileResult],
  totalSuccessCount: Int,
  totalErrorCount: Int,
  totalStatements: Int
)

object ProcessingResult {
  implicit val encoder: Encoder[ProcessingResult] = deriveEncoder[ProcessingResult]
}

object ParseRunner {
  private val logger = Logger("sequala.parse.ParseRunner")

  implicit val sqlBaseObjectReader: TokensReader.Simple[SQLBaseObject] = SQLBaseObjectReader
  implicit val outputFormatReader: TokensReader.Simple[OutputFormat] = OutputFormatReader

  def run(dialect: SQLBaseObject, output: OutputFormat, writeTo: OutputDestination, filePatterns: Seq[String]): Unit = {
    val shouldSuppressLogs = writeTo == ConsoleDestination
    val logLevel = if (shouldSuppressLogs) {
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

    if (shouldSuppressLogs) {
      val loggersToSuppress = Seq("com.arakelian.jq", "com.arakelian.jq.NativeLib", "com.arakelian.jq.JqLibrary")

      loggersToSuppress.foreach { loggerName =>
        val jlogger = JLogger.getLogger(loggerName)
        jlogger.setLevel(JLevel.OFF)
        jlogger.setUseParentHandlers(false)
      }
    }

    val files = filePatterns.flatMap { pattern =>
      val currentDir = Paths.get(".").toAbsolutePath.normalize

      val directFile = if (Paths.get(pattern).isAbsolute) {
        Paths.get(pattern).toFile
      } else {
        currentDir.resolve(pattern).toFile
      }

      if (directFile.exists() && directFile.isFile && !pattern.contains('*') && !pattern.contains('?')) {
        Seq(directFile)
      } else if (pattern.contains('*') || pattern.contains('?')) {
        val normalizedPattern = pattern.replace('\\', '/')

        val parts = normalizedPattern.split("/").filter(_.nonEmpty)
        def findBaseRec(path: java.nio.file.Path, index: Int): (java.nio.file.Path, Int) =
          if (index >= parts.length) {
            (path, index)
          } else {
            val part = parts(index)
            if (part == "..") {
              findBaseRec(path.getParent, index + 1)
            } else if (part == ".") {
              findBaseRec(path, index + 1)
            } else if (part.contains('*') || part.contains('?')) {
              (path, index)
            } else {
              val nextPath = path.resolve(part)
              if (nextPath.toFile.exists() && nextPath.toFile.isDirectory) {
                findBaseRec(nextPath, index + 1)
              } else {
                (path, index)
              }
            }
          }

        val (baseDir, baseIndex) = findBaseRec(currentDir, 0)

        val relativePattern = if (baseIndex < parts.length) {
          parts.slice(baseIndex, parts.length).mkString("/")
        } else {
          "**"
        }

        val basePath = baseDir.normalize

        if (basePath.toFile.exists() && basePath.toFile.isDirectory) {
          val fs = FileSystems.getDefault
          val matcher = fs.getPathMatcher(s"glob:$relativePattern")

          try {
            import scala.jdk.CollectionConverters._
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

    if (files.isEmpty) {
      logger.warn(s"No files found matching: ${filePatterns.mkString(", ")}")
      System.exit(1)
    }

    val result = parseFiles(files, dialect)

    generateOutputs(result, output, writeTo)

    if (result.totalErrorCount > 0) {
      System.exit(1)
    }
  }

  def parseFiles(files: Seq[File], dialect: SQLBaseObject): ProcessingResult = {
    val fileResults = files.map { file =>
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

      val parseResults: Seq[sequala.common.statement.StatementParseResult] = dialect.parseAll(content)

      val statementResults = parseResults.zipWithIndex.map { case (parseResult, idx) =>
        val (statementText, isUnparseable) = parseResult.result match {
          case Right(stmt) => (stmt.toString, false)
          case Left(unparseable) => (unparseable.content, true)
        }

        val absoluteLineNum = if (parseResult.startPos >= 0 && parseResult.startPos < content.length) {
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
        if (stmt.success) (s + 1, e) else (s, e + 1)
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

  def generateOutputs(result: ProcessingResult, output: OutputFormat, writeTo: OutputDestination): Unit = {
    val formattedOutput = output.format(result)
    writeTo.write(formattedOutput)
  }
}
