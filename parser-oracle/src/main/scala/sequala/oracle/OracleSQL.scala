package sequala.oracle

import fastparse.*
import fastparse.ParsingRun
import fastparse.internal.{Msgs, Util}
import scala.io.*
import java.io.*
import sequala.common.statement.*
import sequala.schema.statement.*
import sequala.oracle.*
import sequala.schema.ast.{
  Expression,
  FromElement,
  InsertValues,
  Name,
  OrderBy,
  SelectBody,
  SelectTarget,
  Union,
  WithClause,
  *
}
import sequala.schema.ast.Materialize
import sequala.common.parser.{CommentExtractor, SQLBase, SQLBaseObject, StreamParser}
import fastparse.CharIn
import sequala.schema.{
  AlterTable,
  Cascade,
  Check,
  CheckConstraint,
  CreateIndex,
  CreateView,
  DataType,
  Decimal,
  Delete,
  DoublePrecision,
  DropTable,
  DropView,
  EmptyStatement,
  Explain,
  ForeignKey,
  ForeignKeyConstraint,
  IndexColumn,
  Insert,
  NoAction,
  PrimaryKey,
  PrimaryKeyConstraint,
  Real,
  ReferentialAction,
  Restrict,
  Select,
  SetDefault,
  SetNull,
  SqlBigInt,
  SqlBlob,
  SqlBoolean,
  SqlChar,
  SqlClob,
  SqlDate,
  SqlInteger,
  SqlText,
  SqlTimestamp,
  Statement,
  TableConstraint,
  Unique,
  UniqueConstraint,
  Update,
  VarChar
}
import sequala.schema.oracle.{
  Bytes,
  Chars,
  IntervalDayToSecond,
  IntervalYearToMonth,
  NVarchar2,
  Number,
  OracleChar,
  OracleColumnOptions,
  OracleDataType,
  OracleDropOptions,
  OracleDropViewOptions,
  OracleExplainOptions,
  OracleIndexOptions,
  OracleTableOptions,
  ParserAlterTableAction,
  Raw,
  SizeSemantics,
  Varchar2
}

/** Custom whitespace parser that includes SQL comments (-- ... and /* ... */). This allows comments to be automatically
  * skipped anywhere whitespace is allowed. Based on FastParse's JavaWhitespace pattern.
  */
object OracleWhitespace {
  implicit object whitespace extends Whitespace {
    def apply(ctx: ParsingRun[?]) = {
      val input = ctx.input
      @scala.annotation.tailrec
      def rec(current: Int, state: Int): ParsingRun[Unit] =
        if !input.isReachable(current) then {
          if state == 0 || state == 2 then {
            // Normal whitespace or inside -- comment - both are valid at EOF
            if ctx.verboseFailures then ctx.reportTerminalMsg(current, Msgs.empty)
            ctx.freshSuccessUnit(current)
          } else if state == 1 || state == 3 then {
            // After first '-' or '/' but not a comment - return success at previous position
            if ctx.verboseFailures then ctx.reportTerminalMsg(current - 1, Msgs.empty)
            ctx.freshSuccessUnit(current - 1)
          } else {
            // Inside /* */ comment but reached EOF - this is an error
            ctx.cut = true
            val res = ctx.freshFailure(current)
            if ctx.verboseFailures then ctx.reportTerminalMsg(current, () => Util.literalize("*/"))
            res
          }
        } else {
          val currentChar = input(current)
          (state: @scala.annotation.switch) match {
            case 0 =>
              // Normal whitespace mode
              (currentChar: @scala.annotation.switch) match {
                case ' ' | '\t' | '\n' | '\r' => rec(current + 1, state)
                case '-' => rec(current + 1, state = 1) // Might be start of -- comment
                case '/' => rec(current + 1, state = 3) // Might be start of /* comment
                case _ =>
                  if ctx.verboseFailures then ctx.reportTerminalMsg(current, Msgs.empty)
                  ctx.freshSuccessUnit(current)
              }
            case 1 =>
              // After first '-', check if second '-' follows
              if currentChar == '-' then {
                rec(current + 1, state = 2) // Start of -- comment
              } else {
                // Not a comment, just a single '-', return success at previous position
                if ctx.verboseFailures then ctx.reportTerminalMsg(current - 1, Msgs.empty)
                ctx.freshSuccessUnit(current - 1)
              }
            case 2 =>
              // Inside -- comment, consume until newline
              rec(current + 1, state = if currentChar == '\n' then 0 else state)
            case 3 =>
              // After first '/', check if '*' follows
              (currentChar: @scala.annotation.switch) match {
                case '*' => rec(current + 1, state = 4) // Start of /* comment
                case _ =>
                  // Not a comment, just a single '/', return success at previous position
                  if ctx.verboseFailures then ctx.reportTerminalMsg(current - 1, Msgs.empty)
                  ctx.freshSuccessUnit(current - 1)
              }
            case 4 =>
              // Inside /* */ comment, waiting for '*'
              rec(current + 1, state = if currentChar == '*' then 5 else state)
            case 5 =>
              // Inside /* */ comment, after '*', checking for '/'
              (currentChar: @scala.annotation.switch) match {
                case '/' => rec(current + 1, state = 0) // End of /* comment
                case '*' => rec(current + 1, state = 5) // Stay in state 5 if another '*'
                case _ => rec(current + 1, state = 4) // Go back to state 4 if not '/'
              }
          }
        }
      rec(current = ctx.index, state = 0)
    }
  }
}

/** Oracle SQL dialect implementation. Supports Oracle-specific features including PROMPT, VARCHAR2, NUMBER types, and
  * Oracle CREATE TABLE clauses (TABLESPACE, STORAGE, etc.). For reference, see
  * https://github.com/antlr/grammars-v4/blob/master/sql/plsql/PlSqlParser.g4
  */
class OracleSQL extends SQLBase {
  // Configuration
  type Stmt = Statement
  def caseSensitive = false
  def statementTerminator = "/" // Oracle supports both "/" and ";"
  def supportsCTEs = true
  def supportsReturning = true
  def supportsIfExists = true
  def stringConcatOp = "||"

  // Oracle-specific config
  def supportsPrompt = true
  def supportsVarchar2 = true

  override implicit val whitespaceImplicit: fastparse.Whitespace = OracleWhitespace.whitespace

  // Override multDivOp to prevent "/" from being parsed as division when it's a statement terminator.
  // In Oracle, "/" on its own line is a statement terminator, not division.
  // Division "/" must be followed by an expression operand (identifier, number, paren, etc.)
  // Statement terminator "/" is followed by newline/whitespace leading to EOF or next statement.
  // We use negative lookahead: "/" is NOT division if followed by whitespace then a statement keyword.
  override def multDivOp[$ : P]: P[Unit] = P(
    "*" | ("/" ~ !(&(
      // After whitespace (handled by implicit whitespace parser), check we're NOT at:
      // - End of input
      // - A statement-starting keyword
      End |
        keyword("CREATE") | keyword("SELECT") | keyword("INSERT") | keyword("UPDATE") |
        keyword("DELETE") | keyword("DROP") | keyword("ALTER") | keyword("GRANT") |
        keyword("REVOKE") | keyword("COMMIT") | keyword("EXPLAIN") | keyword("WITH") |
        keyword("BEGIN") | keyword("DECLARE") | keyword("SET") | keyword("PROMPT") |
        keyword("RENAME") | keyword("COMMENT")
    )))
  )

  // Helper parser for digits that allows whitespace consumption
  // Note: We don't override base digits since it returns P[Unit], but we need P[String] here
  def oracleDigits[$ : P]: P[String] = P(CharsWhileIn("0-9").!)

  // Helper parser for NUMBER precision/scale that allows * for unspecified precision
  def numberPrecision[$ : P]: P[String] = P("*".! | oracleDigits)

  // Oracle-specific identifier parser that works with OracleWhitespace
  // Override base identifier to use Oracle-specific parsing
  // Note: In Oracle, keywords can be used as identifiers in many contexts (especially after dots)
  // So we allow keywords as identifiers, but prefer quoted identifiers when ambiguous
  override def identifier[$ : P]: P[Name] = P(
    (("`" ~~/ CharsWhile(_ != '`').! ~ "`")
      | ("\"" ~~/ CharsWhile(_ != '"').! ~~ "\"")).map(Name(_, true: scala.Boolean))
      | (CharIn("_a-zA-Z") ~~ CharsWhileIn("a-zA-Z0-9_").?).!.map(Name(_))
  )

  // Oracle-specific parser for table names with optional database links (schema.table@link)
  def oracleQualifiedTableName[$ : P]: P[Name] = P((identifier ~~ ("." ~/ identifier).? ~~ ("@" ~/ identifier).?).map {
    case (x: Name, y: Option[Name], link: Option[Name]) =>
      (x, y, link) match {
        case (x, None, None) => x
        case (x, Some(y), None) => Name(x.name + "." + y.name, x.quoted || y.quoted)
        case (x, None, Some(link)) => Name(x.name + "@" + link.name, x.quoted || link.quoted)
        case (x, Some(y), Some(link)) =>
          Name(x.name + "." + y.name + "@" + link.name, x.quoted || y.quoted || link.quoted)
      }
  })

  // Oracle-specific parser for dotted pairs with optional database links (schema.table@link)
  def oracleDottedPair[$ : P]: P[(Option[Name], Name)] = P(
    (identifier ~~ ("." ~/ identifier).? ~~ ("@" ~/ identifier).?).map {
      case (x: Name, y: Option[Name], link: Option[Name]) =>
        (x, y, link) match {
          case (x, None, None) => (None, x)
          case (x, Some(y), None) => (Some(x), y)
          case (x, None, Some(link)) => (None, Name(x.name + "@" + link.name, x.quoted || link.quoted))
          case (x, Some(y), Some(link)) =>
            (Some(x), Name(y.name + "@" + link.name, y.quoted || link.quoted))
        }
    }
  )

  // Override alias to use rawIdentifier which checks for keywords
  // This prevents clause keywords like WHERE from being parsed as aliases
  override def alias[$ : P]: P[Name] = P(keyword("AS").? ~ rawIdentifier)

  // Override simpleFromElement to support Oracle database links
  // Note: For subqueries, alias is optional in Oracle (unlike base class which requires it)
  override def simpleFromElement[$ : P]: P[FromElement] = P(
    (("(" ~ select ~ ")" ~ alias.?).map {
      case (body, Some(alias)) => FromSelect(body, alias)
      case (body, None) =>
        FromSelect(body, Name("")) // Anonymous alias for subqueries without alias
    })
      | ((oracleDottedPair ~ alias.?).map { case (schema, table, alias) =>
        FromTable(schema, table, alias)
      })
      | (("(" ~ fromElement ~ ")" ~ alias.?).map {
        case (from, None) => from
        case (from, Some(alias)) => from.withAlias(alias)
      })
  )

  // Override column to use oracleDottedPair for proper Oracle identifier parsing
  override def column[$ : P]: P[Column] = P(oracleDottedPair.map(x => Column(x._2, x._1)))

  // Override leaf to allow keywords as function names (e.g., REPLACE(...))
  override def leaf[$ : P]: P[Expression] = P(
    parens |
      primitive |
      jdbcvar |
      caseWhen | ifThenElse |
      cast |
      nullLiteral |
      // Window functions: function(...) OVER (...) - parse function then check for OVER
      (function ~ (keyword("OVER") ~/
        "(" ~/
        (
          (keyword("PARTITION") ~/ keyword("BY") ~/ expressionList).? ~
            (keyword("ORDER") ~/ keyword("BY") ~/ orderBy
              .rep(sep = comma, min = 1)
              .map(_.toSeq)).?
        ) ~
        ")").?).map {
        case (func, Some((partitionByOpt, orderByOpt))) =>
          WindowFunction(func, partitionByOpt, orderByOpt.getOrElse(Seq()))
        case (func, None) =>
          func
      } |
      // Try column first to avoid conflicts with quoted identifiers
      column |
      // Allow keywords as function names when followed by (
      // Use lookahead that explicitly excludes quoted identifiers (starts with " or `)
      &(!CharIn("\"`") ~ CharIn("_a-zA-Z") ~ CharsWhileIn("a-zA-Z0-9_").? ~ "(") ~ function
  )

  // Override parens to handle subqueries: (SELECT ...)
  override def parens[$ : P]: P[Expression] = P(
    // Try subquery first: (SELECT ...)
    ("(" ~/ &(keyword("SELECT")) ~/ select ~ ")").map(Subquery(_)) |
      // Fall back to regular parenthesized expression: (expression)
      ("(" ~ expression ~ ")")
  )

  // Window function parser: function(...) OVER (PARTITION BY ... ORDER BY ...)
  def windowFunction[$ : P]: P[WindowFunction] = P(
    function ~
      keyword("OVER") ~/
      "(" ~/
      (
        (keyword("PARTITION") ~/ keyword("BY") ~/ expressionList).? ~
          (keyword("ORDER") ~/ keyword("BY") ~/ orderBy
            .rep(sep = comma, min = 1)
            .map(_.toSeq)).?
      ) ~
      ")"
  ).map { case (func, (partitionByOpt, orderByOpt)) =>
    WindowFunction(func, partitionByOpt, orderByOpt.getOrElse(Seq()))
  }

  // Override function to allow keywords as function names
  // But explicitly reject quoted identifiers (they should be columns, not functions)
  override def function[$ : P]: P[Function] = P(
    // First check that we don't have a quoted identifier
    !CharIn("\"`") ~
      (identifier ~ "(" ~/
        keyword("DISTINCT").!.?.map {
          _ != None
        } ~
        ("*".!.map(_ => None)
          | expressionList.map(Some(_))) ~ ")").map { case (name, distinct, args) =>
        Function(name, args, distinct)
      }
  )

  // Parser implementations
  override def dialectSpecificStatement[$ : P]: P[OracleStatement] = P(
    prompt | commit | setScanOff | setScanOn | setDefineOff | grant | revoke | dropSynonym | rename | comment
  )

  def prompt[$ : P]: P[Prompt] = P(
    keyword("PROMPT") ~/ CharsWhile(c => c != '\n' && c != '/' && c != ';').!.map(_.trim)
      .map(Prompt(_))
  )

  def commit[$ : P]: P[Commit] = P(keyword("COMMIT") ~/ Pass.map(_ => Commit()))

  def setDefineOff[$ : P]: P[SetDefineOff] = P(
    keyword("SET") ~ &(keyword("DEFINE")) ~ keyword("DEFINE") ~ keyword("OFF") ~ Pass.map(_ => SetDefineOff())
  )

  def setScanOff[$ : P]: P[SetScanOff] = P(
    keyword("SET") ~ StringInIgnoreCase("SCAN") ~ StringInIgnoreCase("OFF") ~ Pass.map(_ => SetScanOff())
  )

  def setScanOn[$ : P]: P[SetScanOn] = P(
    keyword("SET") ~ StringInIgnoreCase("SCAN") ~ StringInIgnoreCase("ON") ~ Pass.map(_ => SetScanOn())
  )

  def grant[$ : P]: P[Grant] = P(
    keyword("GRANT") ~/
      grantPrivilegeList ~
      keyword("ON") ~/
      oracleQualifiedTableName ~
      keyword("TO") ~/
      identifier.rep(sep = comma, min = 1).map(_.toSeq) ~
      (keyword("WITH") ~/ keyword("GRANT") ~/ StringInIgnoreCase("OPTION")).!.?.map(_.isDefined)
  ).map { case (privileges, onObject, toUsers, withGrantOption) =>
    Grant(privileges, onObject, toUsers, withGrantOption)
  }

  def revoke[$ : P]: P[Revoke] = P(
    keyword("REVOKE") ~/
      grantPrivilegeList ~
      keyword("ON") ~/
      oracleQualifiedTableName ~
      keyword("FROM") ~/
      identifier.rep(sep = comma, min = 1).map(_.toSeq)
  ).map { case (privileges, onObject, fromUsers) =>
    Revoke(privileges, onObject, fromUsers)
  }

  def grantPrivilegeList[$ : P]: P[Seq[String]] = P(grantPrivilege ~ (comma ~/ grantPrivilege).rep).map {
    case (first, rest) => Seq(first) ++ rest.toSeq
  }

  def grantPrivilege[$ : P]: P[String] = P(
    // Multi-word privileges first (must come before single-word to avoid partial matches)
    // Parse "ON COMMIT REFRESH" as a single unit
    (StringInIgnoreCase("ON") ~/ StringInIgnoreCase("COMMIT") ~/ StringInIgnoreCase("REFRESH")).!.map(_ =>
      "ON COMMIT REFRESH"
    ) |
      (StringInIgnoreCase("QUERY") ~/ StringInIgnoreCase("REWRITE")).!.map(_ => "QUERY REWRITE") |
      // Single-word privileges (exclude "ON" and "QUERY" to avoid conflicts)
      StringInIgnoreCase(
        "ALTER",
        "DELETE",
        "INDEX",
        "INSERT",
        "REFERENCES",
        "SELECT",
        "UPDATE",
        "ALL",
        "DEBUG",
        "FLASHBACK"
      ).!
  )

  // Parser for Oracle type names that may be multi-word (e.g., DOUBLE PRECISION, SMALLINT)
  def oracleTypeName[$ : P]: P[Name] = P(
    (keyword("DOUBLE") ~ keyword("PRECISION")).map(_ => Name("DOUBLE PRECISION")) |
      keyword("DOUBLE").map(_ => Name("DOUBLE")) |
      keyword("SMALLINT").map(_ => Name("SMALLINT")) |
      identifier
  )

  // Override oneOrMoreAttributes to use OracleWhitespace instead of MultiLineWhitespace
  override def oneOrMoreAttributes[$ : P]: P[Seq[Name]] = P(
    ("(" ~/ identifier.rep(sep = comma, min = 1) ~ ")")
      | identifier.map(Seq(_))
  )

  // Helper parser for Oracle type parameters (NUMBER(p,s), VARCHAR2(size BYTE|CHAR), etc.)
  // Returns the parameter string (e.g., "(10,2)", "(15 BYTE)")
  def typeParameters[$ : P]: P[String] = P(
    "(" ~/
      (
        // NUMBER(*,0) or NUMBER(3,2) or NUMBER(3,2 BYTE) - precision and scale with optional unit
        // Try this FIRST to avoid backtracking issues with NUMBER(10, 2)
        (numberPrecision ~
          "," ~ numberPrecision ~
          (StringInIgnoreCase("BYTE", "CHAR").!.?)).map { case (size, scale, unit) =>
          val unitStr = unit.map(" " + _).getOrElse("")
          s"($size,$scale$unitStr)"
        } |
          // VARCHAR2(15 BYTE) or NUMBER(3) or NUMBER(3 BYTE) - size with optional unit
          // Parse digits directly to allow whitespace consumption
          (oracleDigits ~
            (StringInIgnoreCase("BYTE", "CHAR").!.?)).map { case (size, unit) =>
            val unitStr = unit.map(" " + _).getOrElse("")
            s"($size$unitStr)"
          }
      ) ~
      ")"
  ).!

  // Oracle-specific table field parser that directly constructs schema types
  def oracleTableField[$ : P]: P[Either[
    TableAnnotation,
    sequala.schema.Column[sequala.schema.oracle.OracleDataType, sequala.schema.oracle.OracleColumnOptions]
  ]] = P(
    (
      keyword("CONSTRAINT") ~
        identifier ~
        (
          (keyword("PRIMARY") ~ keyword("KEY") ~ oneOrMoreAttributes).map(cols => ("pk", cols)) |
            (keyword("UNIQUE") ~ oneOrMoreAttributes).map(cols => ("unique", cols))
        )
    ).map { case (constraintName, (kind, cols)) =>
      kind match
        case "pk" => Left(TablePrimaryKey(cols, Some(constraintName)))
        case "unique" => Left(TableUnique(cols, Some(constraintName)))
    } |
      (keyword("PRIMARY") ~ keyword("KEY") ~ oneOrMoreAttributes).map(cols => Left(TablePrimaryKey(cols))) |
      (keyword("UNIQUE") ~ oneOrMoreAttributes).map(cols => Left(TableUnique(cols))) |
      (keyword("INDEX") ~ keyword("ON") ~ oneOrMoreAttributes).map(cols => Left(TableIndexOn(cols))) |
      (
        identifier ~/
          oracleDataType ~
          oracleColumnAnnotation.rep
      ).map { case (name, dataType, annotations) =>
        val nullable = !annotations.exists(_.isInstanceOf[ColumnIsNotNullable])
        val default = annotations.collectFirst { case ColumnDefaultValue(expr) =>
          import sequala.common.renderer.ParserSqlRenderers.given
          import sequala.schema.{SqlFormatConfig, SqlRenderer}
          given SqlFormatConfig = sequala.schema.SqlFormatConfig.Compact
          summon[SqlRenderer[Expression]].toSql(expr)
        }
        // Check for INVISIBLE annotation
        val invisible = annotations.exists(_.isInstanceOf[ColumnIsInvisible])
        Right(
          sequala.schema.Column(
            name = name.name,
            dataType = dataType,
            nullable = nullable,
            default = default,
            options = sequala.schema.oracle.OracleColumnOptions(virtual = None, invisible = invisible)
          )
        )
      }
  )

  // Positioned version that captures character position for comment attachment
  def positionedOracleTableField[$ : P]: P[
    (
      Int,
      Either[
        TableAnnotation,
        sequala.schema.Column[sequala.schema.oracle.OracleDataType, sequala.schema.oracle.OracleColumnOptions]
      ]
    )
  ] = P(Index ~ oracleTableField)

  // Keep the base tableField for compatibility with base class
  override def tableField[$ : P]: P[Either[TableAnnotation, ColumnDefinition]] = P(
    (
      keyword("CONSTRAINT") ~
        identifier ~
        (
          (keyword("PRIMARY") ~ keyword("KEY") ~ oneOrMoreAttributes).map(cols => ("pk", cols)) |
            (keyword("UNIQUE") ~ oneOrMoreAttributes).map(cols => ("unique", cols))
        )
    ).map { case (constraintName, (kind, cols)) =>
      kind match
        case "pk" => Left(TablePrimaryKey(cols, Some(constraintName)))
        case "unique" => Left(TableUnique(cols, Some(constraintName)))
    } |
      (keyword("PRIMARY") ~ keyword("KEY") ~ oneOrMoreAttributes).map(cols => Left(TablePrimaryKey(cols))) |
      (keyword("UNIQUE") ~ oneOrMoreAttributes).map(cols => Left(TableUnique(cols))) |
      (keyword("INDEX") ~ keyword("ON") ~ oneOrMoreAttributes).map(cols => Left(TableIndexOn(cols))) |
      (
        identifier ~/
          oracleTypeName ~
          typeParameters.? ~
          oracleColumnAnnotation.rep
      ).map { case (name, typeName, paramsOpt, annotations) =>
        val fullTypeName = Name(typeName.name + paramsOpt.getOrElse(""))
        Right(ColumnDefinition(name, fullTypeName, Seq(), annotations))
      }
  )

  // Override columnAnnotation to handle Oracle-specific ENABLE/DISABLE keywords and DEFAULT ON NULL
  def oracleColumnAnnotation[$ : P]: P[ColumnAnnotation] = P(
    // Try DEFAULT ON NULL first (Oracle-specific) - use lookahead to check full sequence
    (&(keyword("DEFAULT") ~ keyword("ON") ~ keyword("NULL")) ~
      keyword("DEFAULT") ~/
      keyword("ON") ~/
      keyword("NULL") ~/
      (
        ("(" ~ expression ~ ")")
          | function // Function calls with parentheses
          | functionCallNoParens // Function calls without parentheses (CURRENT_TIMESTAMP, etc.)
          | primitive
      )).map(ColumnDefaultValue(_)) |
      // DEFAULT NULL (without ON) - use lookahead to ensure NULL follows
      (&(keyword("DEFAULT") ~ keyword("NULL")) ~
        keyword("DEFAULT") ~/
        keyword("NULL")).map(_ => ColumnDefaultValue(NullPrimitive())) |
      // INVISIBLE column option (Oracle-specific) - must come before NULL to avoid conflict
      keyword("INVISIBLE").map(_ => ColumnIsInvisible()) |
      // ENABLE/DISABLE/VALIDATE/NOVALIDATE are Oracle constraint state keywords
      keyword("ENABLE").map(_ => ColumnConstraintEnable()) |
      keyword("DISABLE").map(_ => ColumnConstraintDisable()) |
      keyword("VALIDATE").map(_ => ColumnConstraintValidate()) |
      keyword("NOVALIDATE").map(_ => ColumnConstraintNovalidate()) |
      // Standalone NULL annotation (Oracle allows explicit NULL, though it's redundant)
      keyword("NULL").map(_ => ColumnIsNullable()) |
      // Fall back to standard column annotations (including regular DEFAULT and NOT NULL)
      columnAnnotation
  )

  // Override common statements to return generic Statement types
  override def insert[$ : P]: P[Statement] = P(
    (
      keyword("INSERT") ~/
        (
          keyword("OR") ~/
            keyword("REPLACE")
        ).!.?.map { case None => false; case _ => true } ~
        keyword("INTO") ~/
        oracleQualifiedTableName ~
        ("(" ~/
          identifier ~
          (comma ~/ identifier).rep ~
          ")").map(x => Seq(x._1) ++ x._2).? ~
        (
          // WITH ... SELECT
          (&(keyword("WITH")) ~/ withSelect.map { case (ctes, body) => SelectInsert(body) }) |
            // SELECT (without WITH)
            (&(keyword("SELECT")) ~/ select.map(SelectInsert(_))) |
            // VALUES
            (&(keyword("VALUES")) ~/ valueList)
        )
    ).map { case (orReplace, table, columns, values) =>
      Insert(table, columns, values, orReplace)
    }
  )

  // Parser for SELECT with optional WITH clause
  def withSelect[$ : P]: P[(Seq[WithClause], SelectBody)] = P(withClauseList ~ select).map { case (ctes, body) =>
    (ctes, body)
  }

  def withClauseList[$ : P]: P[Seq[WithClause]] = P(
    keyword("WITH") ~/
      withClause.rep(sep = comma, min = 1).map(_.toSeq)
  )

  def withClause[$ : P]: P[WithClause] = P(
    identifier ~
      keyword("AS") ~/
      "(" ~/ select ~ ")"
  ).map { case (name, body) => WithClause(name, body) }

  override def update[$ : P]: P[Statement] = P(
    (
      keyword("UPDATE") ~/
        oracleQualifiedTableName ~
        // Optional table alias: identifier that is not part of a dotted name and is followed by SET
        // Use negative lookahead to ensure we don't match "SET" as an alias
        (!keyword("SET") ~~ identifier).? ~
        keyword("SET") ~/
        (
          dottedPair.map { case (schema, col) =>
            val colName =
              schema.map(s => Name(s.name + "." + col.name, s.quoted || col.quoted)).getOrElse(col)
            colName
          } ~
            "=" ~/
            expression
        ).rep(sep = comma, min = 1) ~
        (
          StringInIgnoreCase("WHERE") ~/
            expression
        ).?
    ).map { case (table, _, set, where) =>
      Update(table, set, where)
    }
  )

  override def delete[$ : P]: P[Statement] = P(
    (
      keyword("DELETE") ~/
        keyword("FROM").!.?.map(_ => ()) ~/ // FROM is optional in Oracle
        oracleQualifiedTableName ~
        (
          keyword("WHERE") ~/
            expression
        ).?
    ).map { case (table, where) => Delete(table, where) }
  )

  override def alterView[$ : P]: P[OracleStatement] = P(
    (
      keyword("ALTER") ~
        keyword("VIEW") ~/
        oracleQualifiedTableName ~
        (
          (keyword("MATERIALIZE").!.map(_ => Materialize(true)))
            | (keyword("DROP") ~
              keyword("MATERIALIZE").!.map(_ => Materialize(false)))
        )
    ).map { case (name, op) => OracleAlterView(name, op) }
  )

  def alterTable[$ : P]: P[Statement] = P(
    (
      keyword("ALTER") ~
        keyword("TABLE") ~/
        oracleQualifiedTableName ~
        (
          alterTableModify |
            alterTableRenameColumn |
            alterTableAdd |
            alterTableDrop // Combined DROP COLUMN and DROP CONSTRAINT parser
        )
    ).map { case (name, action) =>
      AlterTable[OracleDataType, OracleColumnOptions, OracleTableOptions, ParserAlterTableAction](
        name.name,
        Seq(action)
      )
    }
  )

  def alterTableModify[$ : P]: P[ParserAlterTableAction] = P(
    keyword("MODIFY") ~/
      (
        // MODIFY(column type, ...) - with parentheses
        ("(" ~/
          columnModification.rep(sep = comma, min = 1) ~
          ")").map(mods => AlterTableModify(mods)) |
          // MODIFY column type - without parentheses (single or multiple columns)
          (columnModification ~
            (comma ~/ keyword("MODIFY") ~/ columnModification).rep).map { case (first, rest) =>
            AlterTableModify(Seq(first) ++ rest)
          }
      )
  )

  def alterTableRenameColumn[$ : P]: P[ParserAlterTableAction] = P(
    keyword("RENAME") ~/
      keyword("COLUMN") ~/
      identifier ~
      keyword("TO") ~/
      identifier
  ).map { case (oldName, newName) => OracleAlterTableAction.renameColumn(oldName, newName) }

  def alterTableAdd[$ : P]: P[ParserAlterTableAction] = P(
    keyword("ADD") ~/
      (
        // ADD CONSTRAINT constraint_name UNIQUE/PRIMARY KEY/FOREIGN KEY (...)
        (keyword("CONSTRAINT") ~/
          identifier ~
          tableConstraint).map { case (constraintName, constraint) =>
          OracleAlterTableAction.addConstraint(constraintName, constraint)
        } |
          // ADD(column, ...) - with parentheses
          ("(" ~/
            tableField.rep(sep = comma, min = 1) ~
            ")").map { fields =>
            val columns = fields.collect { case Right(colDef) => colDef }
            val annotations = fields.collect { case Left(ann) => ann }
            AlterTableAdd(columns.toSeq, annotations.toSeq)
          } |
          // ADD column - without parentheses (single or multiple columns)
          (tableField ~
            (keyword("ADD") ~/ tableField).rep).map { case (first, rest) =>
            val allFields = Seq(first) ++ rest
            val columns = allFields.collect { case Right(colDef) => colDef }
            val annotations = allFields.collect { case Left(ann) => ann }
            AlterTableAdd(columns, annotations)
          }
      )
  )

  def tableConstraint[$ : P]: P[TableConstraint] = P(
    // UNIQUE (col1, col2, ...)
    (keyword("UNIQUE") ~/
      "(" ~/ identifier.rep(sep = comma, min = 1).map(_.toSeq) ~ ")").map { columns =>
      UniqueConstraint(Unique(None, columns.map(_.name)))
    } |
      // PRIMARY KEY (col1, col2, ...)
      (keyword("PRIMARY") ~/
        keyword("KEY") ~/
        "(" ~/ identifier.rep(sep = comma, min = 1).map(_.toSeq) ~ ")").map { columns =>
        PrimaryKeyConstraint(PrimaryKey(None, columns.map(_.name)))
      } |
      // FOREIGN KEY (col1, ...) REFERENCES table(col1, ...)
      (keyword("FOREIGN") ~/
        keyword("KEY") ~/
        "(" ~/ identifier.rep(sep = comma, min = 1).map(_.toSeq) ~ ")" ~
        keyword("REFERENCES") ~/
        oracleQualifiedTableName ~
        "(" ~/ identifier.rep(sep = comma, min = 1).map(_.toSeq) ~ ")" ~
        (keyword("ON") ~/ keyword("DELETE") ~/ schemaReferentialAction).? ~
        (keyword("ON") ~/ keyword("UPDATE") ~/ schemaReferentialAction).?).map {
        case (columns, refTable, refColumns, onDelete, onUpdate) =>
          ForeignKeyConstraint(
            ForeignKey(
              None,
              columns.map(_.name),
              refTable.name,
              refColumns.map(_.name),
              onUpdate.getOrElse(NoAction),
              onDelete.getOrElse(NoAction)
            )
          )
      }
  )

  def schemaReferentialAction[$ : P]: P[ReferentialAction] = P(
    (keyword("NO") ~/ keyword("ACTION")).map(_ => NoAction) |
      keyword("RESTRICT").map(_ => Restrict) |
      keyword("CASCADE").map(_ => Cascade) |
      (keyword("SET") ~/ keyword("NULL")).map(_ => SetNull) |
      (keyword("SET") ~/ keyword("DEFAULT")).map(_ => SetDefault)
  )

  def alterTableDrop[$ : P]: P[ParserAlterTableAction] = P(
    keyword("DROP") ~/
      (
        // DROP (col1, col2, ...) - multiple columns in parentheses
        ("(" ~/
          identifier.rep(sep = comma, min = 1).map(_.toSeq) ~
          ")").map(cols => OracleAlterTableAction.dropColumns(cols)) |
          // DROP COLUMN col_name - single column
          keyword("COLUMN") ~/ identifier.map { colName =>
            OracleAlterTableAction.dropColumn(colName)
          } |
          // DROP CONSTRAINT constraint_name
          keyword("CONSTRAINT") ~/ identifier.map { constraintName =>
            OracleAlterTableAction.dropConstraint(constraintName)
          }
      )
  )

  def alterTableDropColumn[$ : P]: P[ParserAlterTableAction] = P(
    keyword("DROP") ~/
      keyword("COLUMN") ~/
      identifier
  ).map(colName => OracleAlterTableAction.dropColumn(colName))

  def alterTableDropConstraint[$ : P]: P[ParserAlterTableAction] = P(
    keyword("DROP") ~/
      keyword("CONSTRAINT") ~/
      identifier
  ).map(constraintName => OracleAlterTableAction.dropConstraint(constraintName))

  def columnModification[$ : P]: P[ColumnModification] = P(
    (
      identifier ~/
        // Type is optional - MODIFY column DEFAULT/NOT NULL is valid without type
        // Use lookahead to check if this looks like a type (not a keyword like DEFAULT/NOT)
        (
          // Only parse type if NOT followed by annotation keywords
          !(&(keyword("DEFAULT") | keyword("NOT") | keyword("PRIMARY"))) ~
            (oracleTypeName ~
              typeParameters.?).map { case (typeName, paramsOpt) =>
              Some(Name(typeName.name + paramsOpt.getOrElse("")))
            }
        ).? ~
        oracleColumnAnnotation.rep
    ).map { case (name: Name, typeOpt, annotations) =>
      ColumnModification(name, typeOpt.flatten, annotations)
    }
  )

  override def dropTableOrView[$ : P]: P[Statement] = P(
    (
      keyword("DROP") ~
        keyword("TABLE", "VIEW").!.map(_.toUpperCase) ~/
        ifExists ~
        oracleQualifiedTableName ~
        (keyword("CASCADE") ~/ StringInIgnoreCase("CONSTRAINTS")).!.?.map(_.isDefined) ~
        keyword("PURGE").!.?.map(_.isDefined)
    ).map {
      case ("TABLE", ifExists, name, cascadeConstraints, purge) =>
        DropTable[OracleDropOptions](name.name, ifExists, OracleDropOptions(cascadeConstraints, purge))
      case ("VIEW", ifExists, name, _, _) =>
        DropView[OracleDropViewOptions](name, ifExists, OracleDropViewOptions.empty)
      case (_, _, _, _, _) =>
        throw new Exception("Internal Error")
    }
  )

  def dropSynonym[$ : P]: P[OracleStatement] = P(
    (
      keyword("DROP") ~
        keyword("SYNONYM") ~/
        ifExists ~
        oracleQualifiedTableName
    ).map { case (ifExists, name) => OracleDropSynonym(name, ifExists) }
  )

  def rename[$ : P]: P[OracleStatement] = P(
    (
      keyword("RENAME") ~/
        oracleQualifiedTableName ~
        keyword("TO") ~/
        oracleQualifiedTableName
    ).map { case (oldName, newName) => OracleRename(oldName, newName) }
  )

  // Parser for fully qualified column names (schema.table.column)
  def oracleQualifiedColumnName[$ : P]: P[Name] = P(
    (identifier ~ ("." ~/ identifier).rep(min = 1, max = 2)).map { case (first, rest) =>
      val parts = Seq(first) ++ rest.toSeq
      val nameStr = parts.map(_.name).mkString(".")
      val quoted = parts.exists(_.quoted)
      Name(nameStr, quoted)
    }
  )

  def comment[$ : P]: P[OracleStatement] = P(
    keyword("COMMENT") ~/
      keyword("ON") ~/
      (commentOnColumn | commentOnTable)
  )

  def commentOnColumn[$ : P]: P[Comment] = P(
    (
      keyword("COLUMN") ~/
        oracleQualifiedColumnName ~
        keyword("IS") ~/
        quotedString
    ).map { case (columnName, commentText) => Comment(columnName, commentText) }
  )

  def commentOnTable[$ : P]: P[TableComment] = P(
    (
      keyword("TABLE") ~/
        oracleQualifiedTableName ~
        keyword("IS") ~/
        quotedString
    ).map { case (tableName, commentText) => TableComment(tableName, commentText) }
  )

  // PL/SQL recursive parser combinators for blocks and loops
  // Uses FastParse's recursive descent to naturally handle nesting

  // Parse a PL/SQL label: <<label>>
  def plsqlLabel[$ : P]: P[String] = P("<<" ~/ identifier.map(_.name) ~ ">>")

  // Parse DECLARE section (captured as string for now)
  def plsqlDeclareSection[$ : P]: P[String] = P(
    keyword("DECLARE") ~/
      // Parse until BEGIN, handling nested structures
      (!(&(keyword("BEGIN")) ~/ !CharIn("a-zA-Z0-9_")) ~ AnyChar).rep.!
  )

  // Parse EXCEPTION handlers (captured as string for now)
  def plsqlExceptionSection[$ : P]: P[String] = P(
    keyword("EXCEPTION") ~/
      // Parse until END, handling nested structures
      (!(&(keyword("END")) ~/ !CharIn("a-zA-Z0-9_")) ~ AnyChar).rep.!
  )

  // Parse FOR loop parameter: variable IN (SELECT ...) or variable IN 1..10
  def plsqlForLoopParam[$ : P]: P[(Name, Either[SelectBody, String])] = P(
    identifier ~
      keyword("IN") ~/
      (
        // Cursor query: FOR rec IN (SELECT ...) - try this first!
        ("(" ~/ select ~ ")").map(query => Left(query)) |
          // Numeric range: FOR i IN 1..10 or FOR i IN REVERSE 1..10
          (keyword("REVERSE").!.? ~
            expression ~ ".." ~ expression).map { case (reverse, lower, upper) =>
            Right(s"${if reverse.isDefined then "REVERSE " else ""}$lower..$upper")
          } |
          // Cursor name: FOR rec IN cursor_name
          identifier.map(cursorName => Right(cursorName.name))
      )
  )

  // Parse WHILE loop condition
  def plsqlWhileCondition[$ : P]: P[String] = P(
    // Parse expression until LOOP keyword
    (!(&(keyword("LOOP")) ~/ !CharIn("a-zA-Z0-9_")) ~ AnyChar).rep.!
  )

  // Parse a single PL/SQL statement - defined first but references blocks/loops below
  // FastParse handles forward references through lazy evaluation
  def plsqlStatement[$ : P]: P[String] = P(
    // Try to parse structured constructs first (they're recursive)
    plsqlBlock.map(_.toString) |
      plsqlLoop.map(_.toString) |
      // Otherwise, parse a simple statement until semicolon or end of block
      (!(keyword("END") | keyword("EXCEPTION") | keyword("ELSE") | keyword("ELSIF")) ~
        !(&(keyword("END") ~ keyword("LOOP")) ~/ !CharIn("a-zA-Z0-9_")) ~
        AnyChar).rep(1).! ~ (";".? | Pass)
  )

  // Parse sequence of statements (recursive)
  def plsqlStatementSequence[$ : P]: P[String] = P(plsqlStatement.rep(sep = Pass).map(_.mkString(" ")))

  // Parse PL/SQL loop (FOR or WHILE) - simplified for testing
  def plsqlLoop[$ : P]: P[OracleStatement] = P(
    // FOR loop
    (keyword("FOR") ~/
      plsqlForLoopParam ~
      keyword("LOOP") ~/
      plsqlStatementSequence ~
      keyword("END") ~/
      keyword("LOOP") ~
      ";".?).map { case (varName, queryOrRange, statements) =>
      queryOrRange match {
        case Left(query) =>
          PlSqlForLoop(varName, query, statements.trim)
        case Right(rangeStr) =>
          PlSqlForLoop(
            varName,
            SelectBody(
              distinct = false,
              target = Seq(),
              from = Seq(),
              where = None,
              groupBy = None,
              having = None,
              orderBy = Seq(),
              limit = None,
              offset = None,
              union = None
            ),
            s"$rangeStr\n$statements".trim
          )
      }
    } |
      // WHILE loop
      (keyword("WHILE") ~/
        plsqlWhileCondition ~
        keyword("LOOP") ~/
        plsqlStatementSequence ~
        keyword("END") ~/
        keyword("LOOP") ~
        ";".?).map { case (condition, statements) =>
        PlSqlWhileLoop(condition.trim, statements.trim)
      }
  )

  // Parse PL/SQL anonymous block (recursive)
  def plsqlBlock[$ : P]: P[OracleStatement] = P(
    // Optional DECLARE section
    (keyword("DECLARE") ~/ plsqlDeclareSection).? ~
      keyword("BEGIN") ~/
      // Parse sequence of statements (can contain nested blocks/loops)
      plsqlStatementSequence ~
      // Optional EXCEPTION section
      (keyword("EXCEPTION") ~/ plsqlExceptionSection).? ~
      keyword("END") ~
      (plsqlLabel | Pass).? ~
      ";".?
  ).map { case (declareOpt: Option[String], statements: String, exceptionOpt: Option[String], _) =>
    val declareStr = declareOpt.getOrElse("")
    val exceptionStr = exceptionOpt.getOrElse("")
    val content = if declareStr.nonEmpty then s"$declareStr\n$statements" else statements
    val finalContent = if exceptionStr.nonEmpty then s"$content\n$exceptionStr" else content
    PlSqlBlock(finalContent.trim)
  }

  // Main PL/SQL block parser entry point - try plsqlBlock first
  def plSqlBlock[$ : P]: P[OracleStatement] = P(plsqlBlock | plsqlLoop)

  def createSynonym[$ : P]: P[OracleStatement] = P(
    (
      keyword("CREATE") ~
        orReplace ~
        keyword("SYNONYM") ~/
        oracleQualifiedTableName ~
        keyword("FOR") ~/
        oracleQualifiedTableName
    ).map { case (orReplace, name, target) =>
      OracleCreateSynonym(name, orReplace, target)
    }
  )

  override def createIndex[$ : P]: P[Statement] = P(
    (
      keyword("CREATE") ~
        keyword("UNIQUE").!.?.map(_.isDefined) ~
        keyword("INDEX") ~/
        oracleQualifiedTableName ~
        keyword("ON") ~/
        oracleQualifiedTableName ~
        "(" ~/ identifier.rep(sep = comma, min = 1).map(_.toSeq) ~ ")"
    ).map { case (unique, indexName, tableName, columns) =>
      CreateIndex[OracleIndexOptions](
        indexName.name,
        tableName.name,
        columns.map(c => IndexColumn(c.name)),
        unique,
        false,
        OracleIndexOptions.empty
      )
    }
  )

  override def explainStatement[$ : P]: P[Statement] = P(
    keyword("EXPLAIN") ~ select.map(s => Explain[OracleExplainOptions](s, OracleExplainOptions.empty))
  )

  override def createView[$ : P]: P[Stmt] = P(
    (
      keyword("CREATE") ~
        orReplace ~
        // MATERIALIZED and TEMPORARY are mutually exclusive with FORCE/EDITIONABLE
        (keyword("MATERIALIZED").map(_ => (true, false, false, false)) |
          keyword("TEMPORARY").map(_ => (false, true, false, false)) |
          // Regular view: parse optional FORCE and EDITIONABLE keywords
          (
            // Both keywords: FORCE EDITIONABLE
            (StringInIgnoreCase("FORCE") ~ StringInIgnoreCase("EDITIONABLE")).map { _ =>
              (false, false, true, true)
            } |
              // Both keywords: EDITIONABLE FORCE
              (StringInIgnoreCase("EDITIONABLE") ~ StringInIgnoreCase("FORCE")).map { _ =>
                (false, false, true, true)
              } |
              // Just FORCE
              StringInIgnoreCase("FORCE").map(_ => (false, false, true, false)) |
              // Just EDITIONABLE
              StringInIgnoreCase("EDITIONABLE").map(_ => (false, false, false, true)) |
              // No keywords
              Pass.map(_ => (false, false, false, false))
          )) ~
        keyword("VIEW") ~/
        oracleQualifiedTableName ~
        // Column list only allowed for regular views (not materialized/temporary)
        ("(" ~/
          identifier.rep(sep = comma, min = 1).map(_.toSeq) ~
          ")").? ~
        keyword("AS") ~/
        select
    ).map { case (orReplace, (materialized, temporary, force, editionable), name, columnList, query) =>
      CreateView(
        name,
        orReplace,
        query,
        OracleCreateViewOptions(materialized, temporary, force, editionable, columnList)
      )
    }
  )

  override def statementTerminatorParser[$ : P]: P[Unit] = P(";") | P("/" ~ !"*")
  // Override terminatedStatement to handle unparseable statements
  // NOTE: Does NOT handle End here - that would cause infinite loops with .rep
  override def terminatedStatement[$ : P]: P[Stmt] = P(
    (statement ~ (statementTerminatorParser | (";".! ~ Pass) | Pass)).map { case (stmt, _) => stmt } |
      unparseableStatement // Fallback for parse errors (requires at least one char)
  )

  // Override allStatements to handle statements without terminators at the end
  override def allStatements[$ : P]: P[Seq[Stmt]] = P(Start ~ terminatedStatement.rep ~ (End | Pass)).map(_.toSeq)

  // Parser for empty statements (whitespace/comments only)
  // This matches when only whitespace/comments remain before the terminator
  // Handles both cases: comments ending with ; or / on next line
  def emptyStatement[$ : P]: P[EmptyStatement] = P(
    Pass ~ // Consume any remaining whitespace/comments (including ; at end of comment lines)
      (End.map(_ => EmptyStatement()) // End of input
        | &(CharIn("/;")).map(_ => EmptyStatement())) // Lookahead for terminator without consuming it
  )

  override def statement[$ : P]: P[Stmt] = P(
    Pass ~ // This trims off leading whitespace (including comments)
      ( // Check for WITH clause first (top-level WITH ... SELECT)
        (&(keyword("WITH")) ~/ withSelect.map { case (ctes, body) => Select(body, ctes) }) |
          // Check for PL/SQL blocks before other statements
          plSqlBlock |
          // All other statements (including SELECT without WITH, which will be handled by base class)
          (parenthesizedSelect.map(body => Select(body, Seq.empty))
            | update
            | delete
            | insert
            | createStatement // Delegates to subclass
            | (&(keyword("ALTER")) ~/ (alterTable | alterView))
            | dropTableOrView
            | explainStatement
            | dialectSpecificStatement // Hook for dialect extensions
            | emptyStatement // Accept empty statements (whitespace/comments only) when only terminator remains
          )
      )
  )

  override def createStatement[$ : P]: P[Stmt] = P(
    &(keyword("CREATE")) ~/ (createView | oracleCreateTable | createSynonym | createIndex)
  )

  // Result type for positioned CREATE TABLE parsing
  case class PositionedCreateTable(
    tablePosition: Int,
    columnPositions: Seq[(Int, String)], // (position, columnName)
    statement: Stmt
  )

  // Internal parser that captures positions for comment attachment
  def oracleCreateTablePositioned[$ : P]: P[PositionedCreateTable] = P(
    (
      Index ~ // Capture position before CREATE for table-level comments
        keyword("CREATE") ~
        orReplace ~
        keyword("TABLE") ~/
        schemaQualifiedName ~
        (
          (keyword("AS") ~/ select).map(Left(_))
            | ("(" ~/
              positionedOracleTableField.rep(sep = comma) ~
              ")").map(Right(_))
        ) ~
        oracleTableClauses.?
    ).map {
      case (tablePos, orReplace, (schemaOpt, tableName), Left(query), clauses) =>
        val qualifiedName = schemaOpt.map(s => Name(s"$s.$tableName", quoted = true)).getOrElse(Name(tableName))
        PositionedCreateTable(
          tablePos,
          Seq.empty,
          sequala.schema.CreateTableAs(qualifiedName, orReplace, query).asInstanceOf[Stmt]
        )
      case (tablePos, orReplace, (schemaOpt, tableName), Right(positionedFields), clausesOpt) =>
        val columns = positionedFields.collect { case (_, Right(col)) => col }
        val columnPositions = positionedFields.collect { case (pos, Right(col)) => (pos, col.name) }
        val annotations = positionedFields.collect { case (_, Left(ann)) => ann }

        val tablePks = annotations.collect { case pk: TablePrimaryKey => pk }
        val primaryKey = tablePks.headOption.map { pk =>
          sequala.schema.PrimaryKey(pk.name.map(_.name), pk.columns.map(_.name))
        }

        val indexes = annotations.collect { case TableIndexOn(cols) =>
          sequala.schema.Index(None, cols.map(n => sequala.schema.IndexColumn(n.name)))
        }

        val uniques = annotations.collect { case u: TableUnique =>
          sequala.schema.Unique(u.name.map(_.name), u.columns.map(_.name))
        }

        val foreignKeys = annotations.collect { case fk: TableForeignKey =>
          sequala.schema.ForeignKey(
            name = fk.name.map(_.name),
            columns = fk.columns.map(_.name),
            refTable = fk.refTable.name,
            refColumns = fk.refColumns.map(_.name),
            onUpdate = fk.onUpdate.getOrElse(sequala.schema.NoAction),
            onDelete = fk.onDelete.getOrElse(sequala.schema.NoAction)
          )
        }

        val tableOptions =
          clausesOpt.map(convertOracleTableClausesToOptions).getOrElse(sequala.schema.oracle.OracleTableOptions.empty)

        val table = sequala.schema.Table(
          name = tableName,
          columns = columns,
          primaryKey = primaryKey,
          indexes = indexes,
          foreignKeys = foreignKeys,
          checks = Seq.empty,
          uniques = uniques,
          options = tableOptions,
          schema = schemaOpt
        )

        PositionedCreateTable(
          tablePos,
          columnPositions,
          sequala.schema.CreateTable(table = table, orReplace = orReplace, ifNotExists = false).asInstanceOf[Stmt]
        )
    }
  )

  // Public parser that discards positions (backward compatible)
  def oracleCreateTable[$ : P]: P[Stmt] = oracleCreateTablePositioned.map(_.statement)

  // Helper to convert OracleTableClauses to OracleTableOptions
  private def convertOracleTableClausesToOptions(
    clauses: OracleTableClauses
  ): sequala.schema.oracle.OracleTableOptions = {
    sequala.schema.oracle.OracleTableOptions(
      tablespace = clauses.tablespace,
      pctFree = clauses.pctFree,
      pctUsed = clauses.pctUsed,
      iniTrans = None,
      maxTrans = None,
      storage = clauses.storage.map(convertStorageClauseToSchema),
      logging = clauses.logging,
      compress = clauses.compress.map {
        case "BASIC" => sequala.schema.oracle.OracleCompressBasic
        case _ => sequala.schema.oracle.OracleCompressBasic
      },
      cache = clauses.cache,
      parallel = clauses.parallel,
      rowMovement = clauses.rowMovement
    )
  }

  private def convertStorageClauseToSchema(storage: StorageClause): sequala.schema.oracle.OracleStorageClause = {
    sequala.schema.oracle.OracleStorageClause(
      initial = storage.initial,
      next = storage.next,
      minExtents = storage.minExtents,
      maxExtents = storage.maxExtents,
      pctIncrease = storage.pctIncrease,
      bufferPool = storage.bufferPool
    )
  }

  case class OracleTableClauses(
    tablespace: Option[String] = None,
    pctUsed: Option[Int] = None,
    pctFree: Option[Int] = None,
    storage: Option[StorageClause] = None,
    logging: Option[scala.Boolean] = None,
    compress: Option[String] = None, // Changed to String to capture BASIC/ADVANCED/FOR
    cache: Option[scala.Boolean] = None,
    parallel: Option[sequala.schema.oracle.OracleParallelClause] = None,
    monitoring: Option[scala.Boolean] = None,
    rowMovement: Option[scala.Boolean] = None // Added for ENABLE/DISABLE ROW MOVEMENT
  )

  // Single table clause - can be any of the supported clauses
  private def singleTableClause[$ : P]: P[OracleTableClauses => OracleTableClauses] = P(
    tablespaceClause.map(ts => (c: OracleTableClauses) => c.copy(tablespace = Some(ts)))
      | pctFreeClause.map(pf => (c: OracleTableClauses) => c.copy(pctFree = Some(pf)))
      | pctUsedClause.map(pu => (c: OracleTableClauses) => c.copy(pctUsed = Some(pu)))
      | initransClause.map(_ => (c: OracleTableClauses) => c) // Discard
      | maxtransClause.map(_ => (c: OracleTableClauses) => c) // Discard
      | storageClause.map(st => (c: OracleTableClauses) => c.copy(storage = Some(st)))
      | loggingClause.map(log => (c: OracleTableClauses) => c.copy(logging = Some(log)))
      | compressClause.map(comp => (c: OracleTableClauses) => c.copy(compress = Some(comp)))
      | cacheClause.map(cch => (c: OracleTableClauses) => c.copy(cache = Some(cch)))
      | parallelClause.map(par => (c: OracleTableClauses) => c.copy(parallel = Some(par)))
      | monitoringClause.map(mon => (c: OracleTableClauses) => c.copy(monitoring = Some(mon)))
      | rowMovementClause.map(rm => (c: OracleTableClauses) => c.copy(rowMovement = Some(rm)))
  )

  // Table clauses can appear in any order
  def oracleTableClauses[$ : P]: P[OracleTableClauses] = P(singleTableClause.rep).map { modifiers =>
    modifiers.foldLeft(OracleTableClauses())((clauses, modifier) => modifier(clauses))
  }

  def tablespaceClause[$ : P]: P[String] = P(StringInIgnoreCase("TABLESPACE") ~/ identifier.map(_.name))

  def pctUsedClause[$ : P]: P[Int] = P(StringInIgnoreCase("PCTUSED") ~/ integer.map(_.toInt))

  def pctFreeClause[$ : P]: P[Int] = P(StringInIgnoreCase("PCTFREE") ~/ integer.map(_.toInt))

  def initransClause[$ : P]: P[Unit] = P(StringInIgnoreCase("INITRANS") ~/ integer.map(_ => ()))

  def maxtransClause[$ : P]: P[Unit] = P(StringInIgnoreCase("MAXTRANS") ~/ integer.map(_ => ()))

  def storageClause[$ : P]: P[StorageClause] = P(
    StringInIgnoreCase("STORAGE") ~/ "(" ~/
      storageOptions ~
      ")"
  )

  def storageOptions[$ : P]: P[StorageClause] = P(
    (storageInitial.?
      ~ storageNext.?
      ~ storageMinExtents.?
      ~ storageMaxExtents.?
      ~ storagePctIncrease.?
      ~ storageBufferPool.?).map { case (init, next, min, max, pct, buf) =>
      StorageClause(init, next, min, max, pct, buf)
    }
  )

  def storageInitial[$ : P]: P[String] = P(StringInIgnoreCase("INITIAL") ~/ (integer.! ~ CharIn("KMG").!.?).map {
    case (n, Some(u)) => s"$n$u"
    case (n, None) => n
  })

  def storageNext[$ : P]: P[String] = P(StringInIgnoreCase("NEXT") ~/ (integer.! ~ CharIn("KMG").!.?).map {
    case (n, Some(u)) => s"$n$u"
    case (n, None) => n
  })

  def storageMinExtents[$ : P]: P[Int] = P(StringInIgnoreCase("MINEXTENTS") ~/ integer.map(_.toInt))

  def storageMaxExtents[$ : P]: P[String] = P(
    StringInIgnoreCase("MAXEXTENTS") ~/ (
      StringInIgnoreCase("UNLIMITED").!.map(_ => "UNLIMITED")
        | integer.!.map(_.toString)
    )
  )

  def storagePctIncrease[$ : P]: P[Int] = P(StringInIgnoreCase("PCTINCREASE") ~/ integer.map(_.toInt))

  def storageBufferPool[$ : P]: P[String] = P(
    StringInIgnoreCase("BUFFER_POOL") ~/ (
      StringInIgnoreCase("DEFAULT").!.map(_ => "DEFAULT")
        | identifier.map(_.name)
    )
  )

  def loggingClause[$ : P]: P[scala.Boolean] = P(
    StringInIgnoreCase("LOGGING").map(_ => true) | StringInIgnoreCase("NOLOGGING").map(_ => false)
  )

  def compressClause[$ : P]: P[String] = P(
    (StringInIgnoreCase("COMPRESS") ~/ (
      StringInIgnoreCase("BASIC").map(_ => "BASIC") |
        StringInIgnoreCase("ADVANCED").map(_ => "ADVANCED") |
        (StringInIgnoreCase("FOR") ~/ identifier.map(_.name.toUpperCase))
    ).?).map {
      case Some(compressType) => compressType
      case None => "BASIC"
    } |
      StringInIgnoreCase("NOCOMPRESS").map(_ => "NONE")
  )

  def cacheClause[$ : P]: P[scala.Boolean] = P(
    StringInIgnoreCase("CACHE").map(_ => true) | StringInIgnoreCase("NOCACHE").map(_ => false)
  )

  def parallelClause[$ : P]: P[sequala.schema.oracle.OracleParallelClause] = P(
    (StringInIgnoreCase("PARALLEL") ~/ integer.map(_.toInt).?).map(sequala.schema.oracle.OracleParallelClause(_)) |
      StringInIgnoreCase("NOPARALLEL").map(_ => sequala.schema.oracle.OracleParallelClause(Some(0)))
  )

  def monitoringClause[$ : P]: P[scala.Boolean] = P(
    StringInIgnoreCase("MONITORING").map(_ => true) | StringInIgnoreCase("NOMONITORING").map(_ => false)
  )

  def rowMovementClause[$ : P]: P[scala.Boolean] = P(
    ((StringInIgnoreCase("ENABLE") | StringInIgnoreCase("DISABLE")).! ~/
      StringInIgnoreCase("ROW") ~/
      StringInIgnoreCase("MOVEMENT")).map {
      case "ENABLE" => true
      case "DISABLE" => false
      case _ => false
    }
  )

  def sizeSemantics[$ : P]: P[SizeSemantics] = P(
    (StringInIgnoreCase("BYTE") ~~ !CharIn("a-zA-Z0-9_")).map(_ => Bytes) |
      (StringInIgnoreCase("CHAR") ~~ !CharIn("a-zA-Z0-9_")).map(_ => Chars)
  )

  // Oracle-specific string types
  // Keep all VARCHAR2 types as Oracle Varchar2 for proper round-tripping (default to BYTE semantics)
  private def oracleStringTypes[$ : P]: P[sequala.schema.oracle.OracleDataType] = P(
    (StringInIgnoreCase("VARCHAR2") ~ "(" ~ integer.map(_.toInt) ~ sizeSemantics.? ~ ")")
      .map { case (len, sem) =>
        Varchar2(len, sem.getOrElse(Bytes)): sequala.schema.oracle.OracleDataType
      }
      | (StringInIgnoreCase("NVARCHAR2") ~ "(" ~ integer.map(_.toInt) ~ ")")
        .map(len => NVarchar2(len))
      | (StringInIgnoreCase("CHAR") ~ "(" ~ integer.map(_.toInt) ~ sizeSemantics.? ~ ")")
        .map { case (len, sem) => OracleChar(len, sem.getOrElse(Bytes)) }
      | (StringInIgnoreCase("NCHAR") ~ "(" ~ integer.map(_.toInt) ~ ")")
        .map(len => sequala.schema.oracle.NChar(len))
      | (StringInIgnoreCase("VARCHAR") ~ "(" ~ integer.map(_.toInt) ~ ")")
        .map(VarChar(_))
  )

  // Parser for NUMBER precision - can be integer or * (wildcard for max precision)
  private def numberPrecisionValue[$ : P]: P[Option[Int]] = P("*".map(_ => None) | integer.map(n => Some(n.toInt)))

  // Oracle-specific numeric types
  // Keep all NUMBER types as Oracle Number for proper round-tripping
  private def oracleNumericTypes[$ : P]: P[sequala.schema.oracle.OracleDataType] = P(
    (StringInIgnoreCase("NUMBER") ~
      ("(" ~ numberPrecisionValue ~ ("," ~ integer.map(_.toInt)).? ~ ")").?)
      .map {
        case Some((precOpt, scaleOpt)) => Number(precOpt, scaleOpt): sequala.schema.oracle.OracleDataType
        case None => Number(None, None): sequala.schema.oracle.OracleDataType
      }
      | (StringInIgnoreCase("BINARY_FLOAT") ~~ !CharIn("a-zA-Z0-9_")).map(_ => sequala.schema.oracle.BinaryFloat)
      | (StringInIgnoreCase("BINARY_DOUBLE") ~~ !CharIn("a-zA-Z0-9_")).map(_ => sequala.schema.oracle.BinaryDouble)
      | (StringInIgnoreCase("INTEGER") ~~ !CharIn("a-zA-Z0-9_")).map(_ => SqlInteger)
      | (StringInIgnoreCase("INT") ~~ !CharIn("a-zA-Z0-9_")).map(_ => SqlInteger)
      | (StringInIgnoreCase("BIGINT") ~~ !CharIn("a-zA-Z0-9_")).map(_ => SqlBigInt)
      | (StringInIgnoreCase("SMALLINT") ~~ !CharIn("a-zA-Z0-9_")).map(_ => sequala.schema.SmallInt)
      | (StringInIgnoreCase("DECIMAL") ~ "(" ~ integer.map(_.toInt) ~ "," ~ integer.map(_.toInt) ~ ")")
        .map { case (p, s) => Decimal(p, s) }
      | (StringInIgnoreCase("FLOAT") ~~ !CharIn("a-zA-Z0-9_")).map(_ => Real)
      | (StringInIgnoreCase("DOUBLE") ~ StringInIgnoreCase("PRECISION")).map(_ => DoublePrecision)
      | (StringInIgnoreCase("DOUBLE") ~~ !CharIn("a-zA-Z0-9_")).map(_ => DoublePrecision)
  )

  // Oracle-specific LOB and other types
  private def oracleOtherTypes[$ : P]: P[sequala.schema.oracle.OracleDataType] = P(
    (StringInIgnoreCase("RAW") ~ "(" ~ integer.map(_.toInt) ~ ")").map(len =>
      Raw(len): sequala.schema.oracle.OracleDataType
    )
      | (StringInIgnoreCase("LONG") ~ StringInIgnoreCase("RAW")).map(_ => sequala.schema.oracle.LongRaw)
      | (StringInIgnoreCase("LONG") ~~ !CharIn("a-zA-Z0-9_")).map(_ => sequala.schema.oracle.OracleLong)
      | (StringInIgnoreCase("ROWID") ~~ !CharIn("a-zA-Z0-9_")).map(_ => sequala.schema.oracle.OracleRowid)
      | (StringInIgnoreCase("UROWID") ~ ("(" ~ integer.map(_.toInt) ~ ")").?)
        .map(len => sequala.schema.oracle.URowid(len))
      | (StringInIgnoreCase("XMLTYPE") ~~ !CharIn("a-zA-Z0-9_")).map(_ => sequala.schema.oracle.XMLType)
      | (StringInIgnoreCase("DATE") ~~ !CharIn("a-zA-Z0-9_")).map(_ => SqlDate)
      | (StringInIgnoreCase("TIMESTAMP") ~ ("(" ~ integer.map(_.toInt) ~ ")").?)
        .map(p => SqlTimestamp(p))
      | oracleIntervalTypes
      | (StringInIgnoreCase("BOOLEAN") ~~ !CharIn("a-zA-Z0-9_")).map(_ => SqlBoolean)
      | (StringInIgnoreCase("TEXT") ~~ !CharIn("a-zA-Z0-9_")).map(_ => SqlText)
      | (StringInIgnoreCase("CLOB") ~~ !CharIn("a-zA-Z0-9_")).map(_ => SqlClob)
      | (StringInIgnoreCase("BLOB") ~~ !CharIn("a-zA-Z0-9_")).map(_ => SqlBlob)
  )

  // Oracle INTERVAL types: INTERVAL YEAR[(p)] TO MONTH, INTERVAL DAY[(d)] TO SECOND[(s)]
  private def oracleIntervalTypes[$ : P]: P[sequala.schema.oracle.OracleDataType] = P(
    (StringInIgnoreCase("INTERVAL") ~/
      (
        // INTERVAL YEAR[(precision)] TO MONTH
        (StringInIgnoreCase("YEAR") ~/
          ("(" ~ integer.map(_.toInt) ~ ")").? ~
          StringInIgnoreCase("TO") ~/
          StringInIgnoreCase("MONTH")).map { precision =>
          IntervalYearToMonth(precision): sequala.schema.oracle.OracleDataType
        }
          |
            // INTERVAL DAY[(day_precision)] TO SECOND[(second_precision)]
            (StringInIgnoreCase("DAY") ~/
              ("(" ~ integer.map(_.toInt) ~ ")").? ~
              StringInIgnoreCase("TO") ~/
              StringInIgnoreCase("SECOND") ~/
              ("(" ~ integer.map(_.toInt) ~ ")").?).map { case (dayPrecision, secondPrecision) =>
              IntervalDayToSecond(dayPrecision, secondPrecision): sequala.schema.oracle.OracleDataType
            }
      ))
  )

  // Oracle-specific data type parser that returns OracleDataType
  def oracleDataType[$ : P]: P[sequala.schema.oracle.OracleDataType] = P(
    oracleStringTypes | oracleNumericTypes | oracleOtherTypes
  )

  // Keep the old dataType parser for compatibility
  def dataType[$ : P]: P[DataType] = oracleDataType.asInstanceOf[P[DataType]]
}

object OracleSQL extends SQLBaseObject {
  type Stmt = Statement
  import fastparse.*
  import fastparse.Parsed
  import io.circe.Json
  import sequala.schema.oracle.OracleStatement
  import sequala.schema.ast.SqlComment
  private val latest = new OracleSQL()

  def name: String = "oracle"

  protected def statementTerminator: String = "/"

  override protected def statementTerminatorChars: Set[scala.Char] = Set(';', '/')

  def apply(input: String): Parsed[Stmt] =
    parse(input, latest.terminatedStatement(_))

  def apply(input: Reader): StreamParser[Stmt] =
    new StreamParser[Stmt](parse(_: Iterator[String], latest.terminatedStatement(_), verboseFailures = true), input)

  def statementToJson(stmt: Statement): Json =
    sequala.schema.oracle.codec.OracleCirceCodecs.dialectStatementEncoder(stmt)

  def statementToSql(stmt: Statement, pretty: Boolean = false): String = {
    import sequala.schema.{SqlFormatConfig, SqlRenderer}
    import sequala.oracle.renderer.OracleParserSqlRenderers.given
    given SqlFormatConfig = if pretty then SqlFormatConfig.Pretty else SqlFormatConfig.Compact
    SqlRenderer[Statement].toSql(stmt)
  }

  /** Parse CREATE TABLE statement with source comments attached. Comments immediately before CREATE TABLE are attached
    * to the table. Comments immediately before each column are attached to that column.
    *
    * @param input
    *   SQL source containing CREATE TABLE statement
    * @return
    *   CreateTable with sourceComment fields populated
    */
  def parseCreateTableWithComments(input: String): Either[
    String,
    sequala.schema.CreateTable[
      sequala.schema.oracle.OracleDataType,
      sequala.schema.oracle.OracleColumnOptions,
      sequala.schema.oracle.OracleTableOptions
    ]
  ] = {
    import CommentExtractor.*

    val comments = extract(input)

    parse(input, latest.oracleCreateTablePositioned(_)) match {
      case Parsed.Success(positioned, _) =>
        positioned.statement match {
          case ct: sequala.schema.CreateTable[?, ?, ?] =>
            // positioned.tablePosition may be at start of input (before whitespace)
            // Find actual CREATE keyword position for accurate comment association
            val createRegex = """(?i)\bCREATE\b""".r
            val createPos = createRegex.findFirstMatchIn(input).map(_.start).getOrElse(positioned.tablePosition)
            val tableLine = positionToLine(input, createPos)
            val tableComments = commentsForLine(comments, tableLine)

            val columnsWithComments = ct.table.columns.map { col =>
              val colPos = positioned.columnPositions.find(_._2 == col.name).map(_._1)
              val colLine = colPos.map(p => positionToLine(input, p)).getOrElse(0)
              val colComments = if colLine > 0 then commentsForLine(comments, colLine) else Seq.empty
              col
                .asInstanceOf[
                  sequala.schema.Column[sequala.schema.oracle.OracleDataType, sequala.schema.oracle.OracleColumnOptions]
                ]
                .copy(sourceComment = colComments)
            }

            val tableWithComments = ct.table
              .asInstanceOf[sequala.schema.Table[
                sequala.schema.oracle.OracleDataType,
                sequala.schema.oracle.OracleColumnOptions,
                sequala.schema.oracle.OracleTableOptions
              ]]
              .copy(
                sourceComment = tableComments,
                columns = columnsWithComments.asInstanceOf[Seq[
                  sequala.schema.Column[sequala.schema.oracle.OracleDataType, sequala.schema.oracle.OracleColumnOptions]
                ]]
              )

            Right(
              ct.asInstanceOf[sequala.schema.CreateTable[
                sequala.schema.oracle.OracleDataType,
                sequala.schema.oracle.OracleColumnOptions,
                sequala.schema.oracle.OracleTableOptions
              ]].copy(table = tableWithComments)
            )

          case _ => Left("Parsed statement is not a CreateTable")
        }

      case Parsed.Failure(label, index, extra) =>
        Left(s"Parse error at index $index: $label")
    }
  }

  /** Parse CREATE TABLE statement using instrumentation-based comment attachment. This is a prototype demonstrating the
    * approach where:
    *   1. CommentExtractor extracts all comments with their positions
    *   2. Instrumentation captures parser start positions
    *   3. Comments are matched to parsers by line number
    *   4. Comments are attached to AST nodes via HasSourceComment typeclass
    *
    * @param input
    *   SQL source containing CREATE TABLE statement
    * @return
    *   CreateTable with sourceComment fields populated
    */
  def parseCreateTableWithInstrumentation(input: String): Either[
    String,
    sequala.schema.CreateTable[
      sequala.schema.oracle.OracleDataType,
      sequala.schema.oracle.OracleColumnOptions,
      sequala.schema.oracle.OracleTableOptions
    ]
  ] = {
    import sequala.common.parser.CommentExtractor
    import sequala.common.parser.CommentExtractor.{PositionedComment, positionToLine, commentsForLine}
    import scala.collection.mutable

    val comments = CommentExtractor.extract(input)
    val parserPositions = mutable.Map[String, mutable.Buffer[Int]]()

    val instrument = new fastparse.internal.Instrument {
      def beforeParse(parser: String, index: Int): Unit = {
        parserPositions.getOrElseUpdate(parser, mutable.Buffer.empty) += index
      }
      def afterParse(parser: String, index: Int, success: Boolean): Unit = ()
    }

    parse(input, latest.oracleCreateTable(_), instrument = instrument) match {
      case Parsed.Success(stmt, _) =>
        stmt match {
          case ct: sequala.schema.CreateTable[?, ?, ?] =>
            val typedCt = ct.asInstanceOf[sequala.schema.CreateTable[
              sequala.schema.oracle.OracleDataType,
              sequala.schema.oracle.OracleColumnOptions,
              sequala.schema.oracle.OracleTableOptions
            ]]

            val tablePos = parserPositions.get("oracleCreateTable").flatMap(_.headOption).getOrElse(0)
            val tableLine = positionToLine(input, tablePos)
            val tableComments = commentsForLine(comments, tableLine)

            val fieldPositions = parserPositions.getOrElse("oracleTableField", mutable.Buffer.empty).toSeq

            val columnsWithComments = typedCt.table.columns.map { col =>
              val colPos = fieldPositions.find { pos =>
                val snippet = input.substring(pos, math.min(pos + col.name.length + 5, input.length))
                snippet.toUpperCase.startsWith(col.name.toUpperCase)
              }
              val colLine = colPos.map(positionToLine(input, _)).getOrElse(0)
              val colComments = if colLine > 0 then commentsForLine(comments, colLine) else Seq.empty
              col.copy(sourceComment = colComments)
            }

            val tableWithComments = typedCt.table.copy(sourceComment = tableComments, columns = columnsWithComments)

            Right(typedCt.copy(table = tableWithComments))

          case _ => Left("Parsed statement is not a CreateTable")
        }

      case Parsed.Failure(label, index, extra) =>
        Left(s"Parse error at index $index: $label")
    }
  }
}

// Version-specific subclasses
class Oracle11gSQL extends OracleSQL {
  override def supportsReturning = false // 11g limitation
}

object Oracle11g {
  type Stmt = Statement
  import fastparse.*
  import fastparse.Parsed
  private val instance = new Oracle11gSQL()

  def apply(input: String): Parsed[Stmt] =
    parse(input, instance.terminatedStatement(_))

  def apply(input: Reader): StreamParser[Stmt] =
    new StreamParser[Stmt](parse(_: Iterator[String], instance.terminatedStatement(_), verboseFailures = true), input)
}
