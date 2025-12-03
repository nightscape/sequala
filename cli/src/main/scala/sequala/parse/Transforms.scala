package sequala.parse

import sequala.common.Dialect
import sequala.schema.Statement
import sequala.schema.codec.DialectCodecs
import io.circe.*
import io.circe.syntax.*
import io.circe.parser.*
import scala.io.Source
import java.nio.file.Paths
import mainargs.TokensReader

/** Type alias for statement-level transformations */
type StatementTransform = Seq[Statement] => Seq[Statement]

/** Result of a JQ transform that may include side-effect writes */
case class TransformResult(
  statements: Seq[Statement],
  writes: Map[String, String] // path -> content
)

/** Transform that can produce both statements and side-effect writes */
type TransformWithWrites = Seq[Statement] => TransformResult

/** Factory that produces transforms given a dialect and optional source file paths (for @bind support) */
type TransformFactory = (Dialect, Seq[String]) => Either[String, Seq[StatementTransform]]

/** Factory that produces transforms with write support */
type TransformFactoryWithWrites =
  (Dialect, Seq[String], Boolean) => Either[String, (Seq[StatementTransform], Seq[Statement] => Map[String, String])]

/** Wrapper that stores tokens alongside factory for later retrieval */
case class TransformFactoryWithTokens(factory: TransformFactory, tokens: Seq[String])

object TransformFactoryTokenStore:
  private val tokenMap = scala.collection.mutable.Map[TransformFactory, Seq[String]]()

  def store(factory: TransformFactory, tokens: Seq[String]): Unit =
    tokenMap(factory) = tokens

  def get(factory: TransformFactory): Option[Seq[String]] =
    tokenMap.get(factory)

/** TokensReader for parsing transform specifications from CLI arguments */
given TransformFactoryReader: TokensReader.Simple[TransformFactory] with
  def shortName = "transforms"
  def read(strs: Seq[String]): Either[String, TransformFactory] =
    if strs.isEmpty then Left("Missing transform specification")
    else
      val tokens = strs.head.split(',').map(_.trim).toSeq
      val factory: TransformFactory = (dialect, sourceFilePaths) =>
        Transforms.resolveTransforms(tokens, dialect, sourceFilePaths)
      TransformFactoryTokenStore.store(factory, tokens)
      Right(factory)

object Transforms:

  /** Extract table name from a statement, returning None if not applicable */
  private def getTableName(stmt: Statement): Option[String] =
    stmt match
      case ct: sequala.schema.CreateTable[?, ?, ?] => Some(ct.table.name)
      case cta: sequala.schema.CreateTableAs => Some(cta.name.name)
      case dt: sequala.schema.DropTable[?] => Some(dt.tableName)
      case at: sequala.schema.AlterTable[?, ?, ?, ?] => Some(at.tableName)
      case ci: sequala.schema.CreateIndex[?] => Some(ci.tableName)
      case ins: sequala.schema.Insert => Some(ins.table.name)
      case upd: sequala.schema.Update => Some(upd.table.name)
      case del: sequala.schema.Delete => Some(del.table.name)
      case stc: sequala.schema.SetTableComment => Some(stc.tableName)
      case scc: sequala.schema.SetColumnComment => Some(scc.tableName)
      case _ => None

  /** Check if a statement type should be filtered out as noise */
  private def isNoise(stmt: Statement): Boolean =
    stmt match
      case _: sequala.schema.oracle.SetDefineOff => true
      case _: sequala.schema.oracle.SetScanOff => true
      case _: sequala.schema.oracle.SetScanOn => true
      case _: sequala.schema.oracle.Prompt => true
      case _: sequala.schema.Select => true
      case _: sequala.schema.oracle.Commit => true
      case _ => false

  /** Check if a statement is affected by DROP TABLE ordering */
  private def isDropAffected(stmt: Statement): Boolean =
    stmt match
      case _: sequala.schema.Insert => true
      case _: sequala.schema.Update => true
      case _: sequala.schema.Delete => true
      case _: sequala.schema.AlterTable[?, ?, ?, ?] => true
      case _: sequala.schema.CreateIndex[?] => true
      case _ => false

  /** Get statement type priority for sorting */
  private def getStatementPriority(stmt: Statement): Int =
    stmt match
      case _: sequala.schema.CreateTable[?, ?, ?] => 0
      case _: sequala.schema.CreateTableAs => 0
      case _: sequala.schema.CreateIndex[?] => 1
      case _: sequala.schema.CreateView[?] => 2
      case _: sequala.schema.AlterTable[?, ?, ?, ?] => 4
      case _: sequala.schema.oracle.Grant => 5
      case _: sequala.schema.oracle.Revoke => 6
      case _: sequala.schema.Insert => 7
      case _: sequala.schema.Update => 8
      case _: sequala.schema.Delete => 9
      case _: sequala.schema.SetTableComment => 10
      case _: sequala.schema.SetColumnComment => 10
      case _: sequala.schema.oracle.OracleColumnComment => 10
      case _: sequala.schema.oracle.OracleTableComment => 10
      case _: sequala.schema.DropTable[?] => 13
      case _ => 99

  /** Filter out noise statements */
  def filter(stmts: Seq[Statement]): Seq[Statement] = stmts.filterNot(isNoise)

  /** Remove INSERT/UPDATE/ALTER statements that precede a DROP TABLE for the same table */
  def dropOptimize(stmts: Seq[Statement]): Seq[Statement] =
    // Build map of last DROP TABLE index for each table
    val lastDropIdx = stmts.zipWithIndex
      .filter { case (stmt, _) => stmt.isInstanceOf[sequala.schema.DropTable[?]] }
      .flatMap { case (stmt, idx) => getTableName(stmt).map(_ -> idx) }
      .groupBy(_._1)
      .view
      .mapValues(_.map(_._2).max)
      .toMap

    stmts.zipWithIndex
      .filter { case (stmt, idx) =>
        if isDropAffected(stmt) then
          getTableName(stmt) match
            case Some(tbl) =>
              lastDropIdx.get(tbl) match
                case Some(dropIdx) => idx >= dropIdx // Keep if at or after last DROP
                case None => true // Keep if no DROP for this table
            case None => true // Keep if can't determine table name
        else true // Keep non-drop-affected statements
      }
      .map(_._1)

  /** Keep only the last CREATE TABLE per table name; remove DROP TABLEs for tables being re-created */
  def dedupe(stmts: Seq[Statement]): Seq[Statement] =
    // Collect all table names that are being created
    val createdTables = stmts.flatMap { stmt =>
      stmt match
        case ct: sequala.schema.CreateTable[?, ?, ?] => Some(ct.table.name)
        case cta: sequala.schema.CreateTableAs => Some(cta.name.name)
        case _ => None
    }.toSet

    // Filter out DROP TABLEs for tables we're creating
    val withoutDropsForCreated = stmts.filter { stmt =>
      stmt match
        case dt: sequala.schema.DropTable[?] => !createdTables.contains(dt.tableName)
        case _ => true
    }

    // Build map of last CREATE TABLE per table name (by index)
    val lastCreateIdxMap = withoutDropsForCreated.zipWithIndex
      .flatMap { case (stmt, idx) =>
        stmt match
          case ct: sequala.schema.CreateTable[?, ?, ?] => Some((ct.table.name, idx))
          case cta: sequala.schema.CreateTableAs => Some((cta.name.name, idx))
          case _ => None
      }
      .groupBy(_._1)
      .view
      .mapValues(_.map(_._2).max)
      .toMap

    // Rebuild list: keep only last CREATE per table, keep all other statements
    withoutDropsForCreated.zipWithIndex.flatMap { case (stmt, idx) =>
      getTableName(stmt) match
        case Some(tbl) if lastCreateIdxMap.contains(tbl) =>
          stmt match
            case _: sequala.schema.CreateTable[?, ?, ?] | _: sequala.schema.CreateTableAs =>
              // Only keep if this is the last CREATE for this table
              if idx == lastCreateIdxMap(tbl) then Some(stmt) else None
            case _ => Some(stmt) // Keep non-CREATE statements
        case _ => Some(stmt)
    }

  /** Sort statements by type priority */
  def sort(stmts: Seq[Statement]): Seq[Statement] = stmts.sortBy(getStatementPriority)

  /** Apply a sequence of transforms in order */
  def apply(stmts: Seq[Statement], transforms: Seq[StatementTransform]): Seq[Statement] =
    transforms.foldLeft(stmts)((acc, transform) => transform(acc))

  /** Get a transform by name */
  def byName(name: String, dialect: Dialect): Either[String, StatementTransform] =
    name match
      case "filter" => Right(filter _)
      case "drop-optimize" => Right(dropOptimize _)
      case "dedupe" => Right(dedupe _)
      case "sort" => Right(sort _)
      case s"exclude:$types" =>
        val excluded = types.split(',').map(_.trim).toSet
        Right { stmts =>
          stmts.filterNot { s =>
            val className = s.getClass.getSimpleName.replace("$", "")
            excluded.contains(className)
          }
        }
      case _ => Left(s"Unknown transform: $name")

  /** Load a JQ transform from a reference string */
  def loadJq(ref: String, dialect: Dialect): Either[String, StatementTransform] =
    loadJqWithBindings(ref, dialect, Seq.empty)

  /** Load a JQ transform with optional source file paths for @bind directive resolution */
  def loadJqWithBindings(
    ref: String,
    dialect: Dialect,
    sourceFilePaths: Seq[String]
  ): Either[String, StatementTransform] =
    val codecs = dialect.codecs
    given Encoder[Statement] = codecs.statementEncoder
    given Decoder[Statement] = codecs.statementDecoder

    val scriptContent = ref match
      case s"jq:$scriptName" if !scriptName.startsWith("./") && !scriptName.startsWith("/") =>
        // Bundled resource: try dialect-specific first, then generic
        val dialectSpecific = s"transforms/${dialect.name}_$scriptName.jq"
        val generic = s"transforms/$scriptName.jq"

        Option(getClass.getClassLoader.getResource(dialectSpecific))
          .orElse(Option(getClass.getClassLoader.getResource(generic)))
          .map(url => Source.fromURL(url).mkString)
          .toRight(s"JQ script not found: $scriptName (tried $dialectSpecific and $generic)")
      case s"jq:./$path" =>
        // Relative filesystem path
        val currentDir = Paths.get(".").toAbsolutePath.normalize
        val file = currentDir.resolve(path).toFile
        if file.exists() && file.isFile then Right(Source.fromFile(file).mkString)
        else Left(s"JQ script file not found: ./$path")
      case s"jq:$path" if path.startsWith("/") =>
        // Absolute filesystem path
        val file = Paths.get(path).toFile
        if file.exists() && file.isFile then Right(Source.fromFile(file).mkString)
        else Left(s"JQ script file not found: $path")
      case _ =>
        Left(s"Invalid JQ reference format: $ref (expected jq:scriptname or jq:./path or jq:/absolute/path)")

    scriptContent.flatMap { content =>
      // Parse @bind directives to check if we need per-file processing
      val bindDirectives = JqHelperStatic.parseBindDirectives(content)

      Right { (stmts: Seq[Statement]) =>
        // Serialize statements to JSON
        val json = stmts.asJson

        // If we have @bind directives and source file paths, resolve and inject bindings
        // We wrap statements in an object with bindings: {"statements": [...], "bindings": {...}}
        val enrichedJsonString = if bindDirectives.nonEmpty && sourceFilePaths.nonEmpty then
          // For source transforms, we aggregate bindings from all source files
          // Use the first source file path as the reference for resolving binding paths
          val bindings = sourceFilePaths
            .flatMap { sourcePath =>
              try JqHelperStatic.resolveBindings(bindDirectives, sourcePath)
              catch
                case e: RuntimeException if bindDirectives.forall(_.optional) =>
                  Map.empty[String, Json]
            }
            .groupBy(_._1)
            .map { case (k, vs) => k -> vs.head._2 }

          if bindings.nonEmpty then
            // Wrap statements with bindings in an object
            val wrapped = Json.obj("statements" -> json, "bindings" -> Json.obj(bindings.toSeq*))
            Printer.spaces2.copy(dropNullValues = true).print(wrapped)
          else Printer.spaces2.copy(dropNullValues = true).print(json)
        else Printer.spaces2.copy(dropNullValues = true).print(json)

        // Execute JQ script
        val jqOutput = JqHelperStatic.executeJq(enrichedJsonString, content, s"from script '$ref'")

        // Deserialize result
        parse(jqOutput).flatMap(_.as[Seq[Statement]]) match
          case Right(result) => result
          case Left(error) =>
            throw new RuntimeException(s"Failed to decode JQ output: ${error.getMessage}")
      }
    }

  /** Parse transform string tokens into transform names/references (without resolving) */
  def parseTransformTokens(strs: Seq[String]): Either[String, Seq[String]] =
    if strs.isEmpty then Left("Missing transform specification")
    else Right(strs.head.split(',').map(_.trim).toSeq)

  /** Resolve transform tokens to actual transforms given a dialect and source file paths */
  def resolveTransforms(
    tokens: Seq[String],
    dialect: Dialect,
    sourceFilePaths: Seq[String] = Seq.empty
  ): Either[String, Seq[StatementTransform]] =
    tokens.foldLeft(Right(Seq.empty[StatementTransform]): Either[String, Seq[StatementTransform]]) {
      case (Right(acc), token) =>
        if token.startsWith("jq:") then loadJqWithBindings(token, dialect, sourceFilePaths).map(acc :+ _)
        else byName(token, dialect).map(acc :+ _)
      case (Left(err), _) => Left(err)
    }

  /** Resolve transform tokens with write support - returns transforms and a function to collect writes */
  def resolveTransformsWithWrites(
    tokens: Seq[String],
    dialect: Dialect,
    sourceFilePaths: Seq[String] = Seq.empty,
    pretty: Boolean = false
  ): Either[String, (Seq[StatementTransform], Seq[Statement] => Map[String, String])] =
    var writeCollectors = Seq.empty[Seq[Statement] => Map[String, String]]

    val transforms = tokens.foldLeft(Right(Seq.empty[StatementTransform]): Either[String, Seq[StatementTransform]]) {
      case (Right(acc), token) =>
        if token.startsWith("jq:") then
          loadJqWithWriteSupport(token, dialect, sourceFilePaths, pretty) match
            case Right(transformWithWrites) =>
              val collector: Seq[Statement] => Map[String, String] = { stmts =>
                transformWithWrites(stmts).writes
              }
              writeCollectors = writeCollectors :+ collector
              Right(acc :+ { (stmts: Seq[Statement]) => transformWithWrites(stmts).statements })
            case Left(err) => Left(err)
        else byName(token, dialect).map(acc :+ _)
      case (Left(err), _) => Left(err)
    }

    transforms.map { ts =>
      val writeCollector: Seq[Statement] => Map[String, String] = { stmts =>
        writeCollectors.foldLeft(Map.empty[String, String]) { (acc, collector) =>
          acc ++ collector(stmts)
        }
      }
      (ts, writeCollector)
    }

  /** Load a JQ transform with write support (supports @write-map directives) */
  def loadJqWithWriteSupport(
    ref: String,
    dialect: Dialect,
    sourceFilePaths: Seq[String],
    pretty: Boolean = false
  ): Either[String, TransformWithWrites] =
    val codecs = dialect.codecs
    given Encoder[Statement] = codecs.statementEncoder
    given Decoder[Statement] = codecs.statementDecoder

    val scriptContent = ref match
      case s"jq:$scriptName" if !scriptName.startsWith("./") && !scriptName.startsWith("/") =>
        val dialectSpecific = s"transforms/${dialect.name}_$scriptName.jq"
        val generic = s"transforms/$scriptName.jq"
        Option(getClass.getClassLoader.getResource(dialectSpecific))
          .orElse(Option(getClass.getClassLoader.getResource(generic)))
          .map(url => Source.fromURL(url).mkString)
          .toRight(s"JQ script not found: $scriptName (tried $dialectSpecific and $generic)")
      case s"jq:./$path" =>
        val currentDir = Paths.get(".").toAbsolutePath.normalize
        val file = currentDir.resolve(path).toFile
        if file.exists() && file.isFile then Right(Source.fromFile(file).mkString)
        else Left(s"JQ script file not found: ./$path")
      case s"jq:$path" if path.startsWith("/") =>
        val file = Paths.get(path).toFile
        if file.exists() && file.isFile then Right(Source.fromFile(file).mkString)
        else Left(s"JQ script file not found: $path")
      case _ =>
        Left(s"Invalid JQ reference format: $ref (expected jq:scriptname or jq:./path or jq:/absolute/path)")

    scriptContent.flatMap { content =>
      val bindDirectives = JqHelperStatic.parseBindDirectives(content)
      val writeDirectives = JqHelperStatic.parseWriteDirectives(content)

      Right { (stmts: Seq[Statement]) =>
        val json = stmts.asJson

        val enrichedJsonString = if bindDirectives.nonEmpty && sourceFilePaths.nonEmpty then
          val bindings = sourceFilePaths
            .flatMap { sourcePath =>
              try JqHelperStatic.resolveBindings(bindDirectives, sourcePath)
              catch
                case e: RuntimeException if bindDirectives.forall(_.optional) =>
                  Map.empty[String, Json]
            }
            .groupBy(_._1)
            .map { case (k, vs) => k -> vs.head._2 }

          if bindings.nonEmpty then
            val wrapped = Json.obj("statements" -> json, "bindings" -> Json.obj(bindings.toSeq*))
            Printer.spaces2.copy(dropNullValues = true).print(wrapped)
          else Printer.spaces2.copy(dropNullValues = true).print(json)
        else Printer.spaces2.copy(dropNullValues = true).print(json)

        val jqOutput = JqHelperStatic.executeJq(enrichedJsonString, content, s"from script '$ref'")

        val outputJson = parse(jqOutput).getOrElse {
          throw new RuntimeException(s"Failed to parse JQ output as JSON")
        }

        val statements = outputJson.hcursor.downField("statements").focus match
          case Some(stmtsJson) =>
            stmtsJson.as[Seq[Statement]].getOrElse {
              throw new RuntimeException(s"Failed to decode 'statements' field from JQ output")
            }
          case None =>
            outputJson.as[Seq[Statement]].getOrElse {
              throw new RuntimeException(s"Failed to decode JQ output as statements array")
            }

        val writes = if writeDirectives.nonEmpty then
          val inputPath = sourceFilePaths.headOption.getOrElse("")
          collectWrites(jqOutput, writeDirectives, inputPath, dialect, pretty)
        else Map.empty[String, String]

        TransformResult(statements, writes)
      }
    }

  private def collectWrites(
    jqOutput: String,
    writeDirectives: Seq[WriteDirective],
    inputFilePath: String,
    dialect: Dialect,
    pretty: Boolean
  ): Map[String, String] =
    if writeDirectives.isEmpty then Map.empty
    else
      val json = parse(jqOutput).getOrElse {
        throw new RuntimeException(s"Failed to parse jq output as JSON")
      }

      val codecs = dialect.codecs
      given Decoder[Statement] = codecs.statementDecoder
      val renderers = dialect.renderers

      def formatForExtension(value: Json, filePath: String): String =
        val ext =
          if filePath.contains(".") then filePath.substring(filePath.lastIndexOf('.') + 1).toLowerCase else "sql"
        ext match
          case "sql" =>
            value.as[Seq[Statement]] match
              case Right(statements) =>
                import renderers.given
                import sequala.schema.{SqlFormatConfig, SqlRenderer}
                given SqlFormatConfig = if pretty then SqlFormatConfig.Pretty else SqlFormatConfig.Compact
                statements.map(stmt => SqlRenderer[Statement].toSql(stmt)).mkString("\n")
              case Left(_) =>
                value.as[Statement] match
                  case Right(stmt) =>
                    import renderers.given
                    import sequala.schema.{SqlFormatConfig, SqlRenderer}
                    given SqlFormatConfig = if pretty then SqlFormatConfig.Pretty else SqlFormatConfig.Compact
                    SqlRenderer[Statement].toSql(stmt)
                  case Left(_) =>
                    Printer.spaces2.copy(dropNullValues = true).print(value)
          case "yaml" | "yml" => JqHelperStatic.jsonToYaml(value)
          case "json" => Printer.spaces2.copy(dropNullValues = true).print(value)
          case _ => Printer.spaces2.copy(dropNullValues = true).print(value)

      writeDirectives.flatMap {
        case SingleWriteDirective(varName, pathPattern) =>
          val value = json.hcursor.downField(varName).focus.getOrElse {
            throw new RuntimeException(s"@write directive references missing key '$varName' in jq output")
          }
          val resolvedPath = JqHelperStatic.resolvePathPattern(pathPattern, inputFilePath)
          Seq(resolvedPath -> formatForExtension(value, resolvedPath))

        case MapWriteDirective(varName, pathPattern) =>
          val obj = json.hcursor.downField(varName).focus.flatMap(_.asObject).getOrElse {
            throw new RuntimeException(s"@write-map directive requires '$varName' to be an object in jq output")
          }
          obj.toMap.map { case (key, value) =>
            val resolvedPath = JqHelperStatic.resolvePathPattern(pathPattern, inputFilePath, Some(key))
            resolvedPath -> formatForExtension(value, resolvedPath)
          }
      }.toMap
