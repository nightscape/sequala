package sequala.common.parser

import fastparse.*
import scala.io.*
import java.io.*
import sequala.schema.ast.{
  FromElement,
  FromJoin,
  FromSelect,
  FromTable,
  InsertValues,
  Join,
  Name,
  OrderBy,
  SelectAll,
  SelectBody,
  SelectExpression,
  SelectTable,
  SelectTarget,
  Union,
  WithClause
}
import sequala.schema.{EmptyStatement, Statement, Unparseable}
import sequala.common.statement.*
import sequala.schema.statement.*
import io.circe.{Encoder, Json}
import sequala.schema.ast.{
  Arithmetic,
  BooleanPrimitive,
  CaseWhenElse,
  Cast,
  Column,
  Comparison,
  DoublePrimitive,
  Expression,
  Function,
  Identifier,
  InExpression,
  IsNull,
  JDBCVar,
  LongPrimitive,
  Not,
  NullPrimitive,
  StringPrimitive,
  Subquery,
  WindowFunction
}
import sequala.converter.SchemaConverter
import sequala.schema.{Cascade, DataType, NoAction, ReferentialAction, Restrict, SetDefault, SetNull, Statement}

/** Base trait for SQL dialect parsers. Provides shared parser methods and configuration hooks for dialect-specific
  * behavior.
  */
trait SQLBase {
  // Type members allow dialects to extend the Statement ADT
  type Stmt <: sequala.schema.Statement

  // Helper functions for converting parsed operator strings to enum values
  protected def stringToComparisonOp(s: String): Comparison.Op = s.toUpperCase match
    case "=" | "==" => Comparison.Op.Eq
    case "!=" | "<>" => Comparison.Op.Neq
    case ">" => Comparison.Op.Gt
    case "<" => Comparison.Op.Lt
    case ">=" => Comparison.Op.Gte
    case "<=" => Comparison.Op.Lte
    case "LIKE" => Comparison.Op.Like
    case "NOT LIKE" => Comparison.Op.NotLike
    case "RLIKE" => Comparison.Op.RLike
    case "NOT RLIKE" => Comparison.Op.NotRLike

  protected def stringToArithmeticOp(s: String): Arithmetic.Op = s match
    case "+" => Arithmetic.Op.Add
    case "-" => Arithmetic.Op.Sub
    case "*" => Arithmetic.Op.Mult
    case "/" => Arithmetic.Op.Div
    case "&" => Arithmetic.Op.BitAnd
    case "|" => Arithmetic.Op.BitOr
    case "<<" => Arithmetic.Op.ShiftLeft
    case ">>" => Arithmetic.Op.ShiftRight

  // Configuration for simple variations (abstract, implemented by subclasses)
  def caseSensitive: Boolean
  def statementTerminator: String // ";" or "/"
  def supportsCTEs: Boolean
  def supportsReturning: Boolean
  def supportsIfExists: Boolean
  def stringConcatOp: String // "||" or "+"
  /** Whether this dialect supports schema-qualified table names. When true, both `SCHEMA.TABLE` and `"SCHEMA.TABLE"`
    * are parsed as (schema, table). When false, `"SCHEMA.TABLE"` is treated as a single identifier with a literal dot.
    */
  def supportsSchemas: Boolean = true
  implicit val whitespaceImplicit: fastparse.Whitespace = MultiLineWhitespace.whitespace
  // Expression parsing methods
  // These use `this` to access the SQL parser instance, removing hard-coded ANSISQL.instance references

  // Elements methods - inlined from Elements.scala to allow overriding
  // Note: StringInIgnoreCase is a macro requiring literal strings. SQLBase.reservedWordsSet
  // duplicates this list for runtime use in tests.
  def anyKeyword[$ : P] = P(
    StringInIgnoreCase(
      "ADD",
      "ALL",
      "ALTER",
      "AND",
      "AS",
      "ASC",
      "BEGIN",
      "BETWEEN",
      "BY",
      "CASE",
      "CAST",
      "CHECK",
      "COLUMN",
      "COMMENT",
      "COMMIT",
      "CONSTRAINT",
      "CREATE",
      "DECLARE",
      "DEFAULT",
      "DEFINE",
      "DELETE",
      "DESC",
      "DISABLE",
      "DISTINCT",
      "DOUBLE",
      "DROP",
      "ELSE",
      "ELSIF",
      "ENABLE",
      "END",
      "EXCEPTION",
      "EXISTS",
      "EXPLAIN",
      "FALSE",
      "FOR",
      "FROM",
      "FULL",
      "GROUP",
      "GRANT",
      "HAVING",
      "IF",
      "IN",
      "INDEX",
      "INNER",
      "INSERT",
      "INTO",
      "INVISIBLE",
      "IS",
      "JOIN",
      "KEY",
      "LEFT",
      "LIMIT",
      "LOOP",
      "MATERIALIZE",
      "MATERIALIZED",
      "MODIFY",
      "NATURAL",
      "NOT",
      "NULL",
      "OFF",
      "OFFSET",
      "ON",
      "OR",
      "ORDER",
      "OVER",
      "OUTER",
      "PARTITION",
      "PRECISION",
      "PRIMARY",
      "PROMPT",
      "RENAME",
      "REPLACE",
      "REVERSE",
      "REVOKE",
      "RIGHT",
      "SELECT",
      "SET",
      "SYNONYM",
      "TABLE",
      "TEMPORARY",
      "THEN",
      "TO",
      "TRUE",
      "UNIQUE",
      "UPDATE",
      "UNION",
      "VALIDATE",
      "VALUES",
      "VIEW",
      "WHEN",
      "WHERE",
      "WHILE",
      "WITH",
      "FOREIGN",
      "REFERENCES",
      "CASCADE",
      "RESTRICT",
      "ACTION",
      "PURGE",
      "NOVALIDATE"
    ).! ~~ !CharIn("a-zA-Z0-9_")
  )

  def keyword[$ : P](expected: String*) = P[Unit](
    anyKeyword
      .opaque(expected.mkString(" or "))
      .filter(kw => expected.exists(_.equalsIgnoreCase(kw)))
      .map(_ => ())
  )

  def avoidReservedKeywords[$ : P] = P(!anyKeyword)

  def rawIdentifier[$ : P] = P(
    avoidReservedKeywords ~~
      (CharIn("_a-zA-Z") ~~ CharsWhileIn("a-zA-Z0-9_").?).!.map(Name(_))
  )

  def quotedIdentifier[$ : P] = P(
    (("`" ~~/ CharsWhile(_ != '`').! ~ "`")
      | ("\"" ~~/ CharsWhile(_ != '"').! ~~ "\"")).map(Name(_, quoted = true))
  )

  def identifier[$ : P]: P[Name] = P(rawIdentifier | quotedIdentifier)

  def dottedPair[$ : P]: P[(Option[Name], Name)] = P((identifier ~~ ("." ~~ identifier).?).map {
    case (x, None) => (None, x)
    case (x, Some(y)) => (Some(x), y)
  })

  def dottedWildcard[$ : P]: P[Name] = P(identifier ~~ ".*")

  // Schema-qualified table name (schema.table) as a single Name
  def qualifiedTableName[$ : P]: P[Name] = P((identifier ~~ ("." ~~ identifier).?).map {
    case (x, None) => x
    case (x, Some(y)) => Name(x.name + "." + y.name, x.quoted || y.quoted)
  })

  /** Parse a schema-qualified name (schema.name) returning separate schema and name. Works for tables, views,
    * functions, sequences, and other schema-qualified objects.
    *
    * Handles:
    *   - `NAME` → (None, "NAME")
    *   - `SCHEMA.NAME` → (Some("SCHEMA"), "NAME")
    *   - `"SCHEMA"."NAME"` → (Some("SCHEMA"), "NAME")
    *   - `"SCHEMA.NAME"` → (Some("SCHEMA"), "NAME") when supportsSchemas=true
    *   - `"SCHEMA.NAME"` → (None, "SCHEMA.NAME") when supportsSchemas=false
    */
  def schemaQualifiedName[$ : P]: P[(Option[String], String)] = P(dottedPair.map {
    case (Some(schema), name) =>
      // Already parsed as schema.name (e.g., SCHEMA.NAME or "SCHEMA"."NAME")
      (Some(schema.name), name.name)
    case (None, name) if supportsSchemas && name.name.contains('.') =>
      // Single quoted identifier containing a dot (e.g., "SCHEMA.NAME")
      // Split on first dot to extract schema
      val dotIdx = name.name.indexOf('.')
      (Some(name.name.substring(0, dotIdx)), name.name.substring(dotIdx + 1))
    case (None, name) =>
      // Simple name without schema
      (None, name.name)
  })

  def digits[$ : P] = P(CharsWhileIn("0-9"))
  def plusMinus[$ : P] = P("-" | "+")
  def integral[$ : P] = "0" | CharIn("1-9") ~~ digits.?

  def integer[$ : P] = (plusMinus.? ~~ digits).!.map(_.toLong) ~~ !"." // Fail on a trailing period
  def decimal[$ : P] =
    (plusMinus.? ~~ digits ~~ ("." ~~ digits).? ~~ ("e" ~~ plusMinus.? ~~ digits).?).!.map(_.toDouble)

  def escapeQuote[$ : P] = P("''".!.map(_.replaceAll("''", "'")))
  def escapedString[$ : P] = P((CharsWhile(_ != '\'') | escapeQuote).repX.!.map {
    _.replaceAll("''", "'")
  })
  def quotedString[$ : P] = P("'" ~~ escapedString ~~ "'")

  def comma[$ : P] = P(",")

  def expressionList[$ : P]: P[Seq[Expression]] = P(expression.rep(sep = comma))

  def expression[$ : P]: P[Expression] = P(disjunction)

  def disjunction[$ : P] = P((conjunction ~ (!keyword("ORDER") ~ keyword("OR") ~ conjunction).rep).map { x =>
    x._2.fold(x._1) { (accum, current) =>
      Arithmetic(accum, Arithmetic.Op.Or, current)
    }
  })

  def conjunction[$ : P] = P((negation ~ (keyword("AND") ~ negation).rep).map { x =>
    x._2.fold(x._1) { (accum, current) =>
      Arithmetic(accum, Arithmetic.Op.And, current)
    }
  })

  def negation[$ : P] = P((keyword("NOT").!.? ~ comparison).map {
    case (None, expression) => expression
    case (_, expression) => Not(expression)
  })

  def comparison[$ : P] = P(
    (isNullBetweenIn ~
      (StringInIgnoreCase("=", "==", "!=", "<>", ">", "<", ">=", "<=", "LIKE", "NOT LIKE", "RLIKE", "NOT RLIKE").! ~
        addSub).?).map {
      case (expression, None) => expression
      case (lhs, Some((op, rhs))) => Comparison(lhs, stringToComparisonOp(op), rhs)
    }
  )

  def optionalNegation[$ : P]: P[Expression => Expression] = P(keyword("NOT").!.?.map { x =>
    if x.isDefined then { y => Not(y) }
    else { y => y }
  })

  def isNullBetweenIn[$ : P] = P(
    (addSub ~ (
      // IS [NOT] NULL -> IsNull(...)
      (keyword("IS") ~ optionalNegation ~
        keyword("NULL").map(_ => IsNull(_))) | (
        // [IS] [NOT] BETWEEN low AND high
        keyword("IS").? ~ optionalNegation ~
          (
            keyword("BETWEEN") ~
              addSub ~ keyword("AND") ~ addSub
          ).map { case (low, high) =>
            (lhs: Expression) =>
              Arithmetic(
                Comparison(lhs, Comparison.Op.Gte, low),
                Arithmetic.Op.And,
                Comparison(lhs, Comparison.Op.Lte, high)
              )
          }
      ) | (
        optionalNegation ~ keyword("IN") ~/ (
          (
            // IN ( SELECT ... )
            &("(" ~ keyword("SELECT")) ~/
              "(" ~/ this.select.map { query =>
                InExpression(_: Expression, Right(query))
              } ~ ")"
          ) | (
            // IN ('list', 'of', 'items')
            "(" ~/
              expressionList.map(exprs => InExpression(_: Expression, Left(exprs))) ~
              ")"
          )
        )
      )
    ).?).map {
      case (expression, None) => expression
      case (expression, Some((neg, build))) =>
        val f: Expression => Expression = neg.asInstanceOf[Expression => Expression]
        f(build(expression))
    }
  )

  def addSub[$ : P] = P((multDiv ~ ((CharIn("+\\-&\\|") | StringIn("<<", ">>")).! ~ multDiv).rep).map { x =>
    x._2.foldLeft(x._1: Expression) { (accum, current) =>
      Arithmetic(accum, stringToArithmeticOp(current._1), current._2)
    }
  })

  def multDivOp[$ : P] = P(CharIn("*/"))

  def multDiv[$ : P] = P((leaf ~ (multDivOp.! ~ leaf).rep).map { x =>
    x._2.foldLeft(x._1) { (accum, current) =>
      Arithmetic(accum, stringToArithmeticOp(current._1), current._2)
    }
  })

  def leaf[$ : P]: P[Expression] = P(
    parens |
      primitive |
      jdbcvar |
      caseWhen | ifThenElse |
      cast |
      nullLiteral |
      // need to lookahead `function` to avoid conflicts with `column`
      &(identifier ~ "(") ~ function |
      column
  )

  def parens[$ : P] = P("(" ~ expression ~ ")")

  def primitive[$ : P] = P(
    integer.map(v => LongPrimitive(v))
      | decimal.map(v => DoublePrimitive(v))
      | quotedString.map(v => StringPrimitive(v))
      | keyword("TRUE").map(_ => BooleanPrimitive(true))
      | keyword("FALSE").map(_ => BooleanPrimitive(false))
  )

  def column[$ : P] = P(dottedPair.map(x => Column(x._2, x._1)))

  def nullLiteral[$ : P] = P(keyword("NULL").map(_ => NullPrimitive()))

  def function[$ : P] = P(
    (identifier ~ "(" ~/
      keyword("DISTINCT").!.?.map {
        _ != None
      } ~
      ("*".!.map(_ => None)
        | expressionList.map(Some(_))) ~ ")").map { case (name, distinct, args) =>
      Function(name, args, distinct)
    }
  )

  def jdbcvar[$ : P] = P("?".!.map(_ => JDBCVar()))

  def caseWhen[$ : P] = P(
    keyword("CASE") ~/
      (!keyword("WHEN") ~ expression).? ~
      (
        keyword("WHEN") ~/
          expression ~
          keyword("THEN") ~/
          expression
      ).rep ~
      keyword("ELSE") ~/
      expression ~
      keyword("END")
  ).map { case (target, whenThen, orElse) =>
    CaseWhenElse(target, whenThen, orElse)
  }

  def ifThenElse[$ : P] = P(
    keyword("IF") ~/
      expression ~/
      keyword("THEN") ~/
      expression ~/
      keyword("ELSE") ~/
      expression ~/
      keyword("END")
  ).map { case (condition, thenClause, elseClause) =>
    CaseWhenElse(None, Seq(condition -> thenClause), elseClause)
  }

  def cast[$ : P] = P(
    (
      keyword("CAST") ~/ "(" ~/
        expression ~ keyword("AS") ~/
        identifier ~ ("," ~ quotedString).? ~ ")"
    ).map { case (expression, t, format) =>
      Cast(expression, t, format)
    }
  )

  def statementTerminatorParser[$ : P]: P[Unit] = P(statementTerminator)

  // Parser for unparseable statements (error recovery)
  // Consumes everything up to and including the next statement terminator
  // IMPORTANT: Must match at least one char to avoid infinite loops with .rep
  def unparseableStatement[$ : P]: P[Unparseable] = P(
    // Consume at least one char, then everything up to (but not including) the terminator
    AnyChar.! ~ CharsWhile(c => c != statementTerminator.head).! ~ statementTerminatorParser.!
  ).map { case (first, rest, terminator) => Unparseable((first + rest + terminator).trim) }

  // Shared parsers with conditional logic
  // Base implementation includes unparseable fallback - can be overridden by dialects that need special handling
  def terminatedStatement[$ : P]: P[Stmt] = P(
    (statement ~/ statementTerminatorParser) |
      unparseableStatement.map(_.asInstanceOf[Stmt]) // Fallback for parse errors (requires at least one char)
  )

  def statement[$ : P]: P[Stmt] =
    P(
      Pass ~ // This trims off leading whitespace
        (parenthesizedSelect.map(s => sequala.schema.Select(s).asInstanceOf[Stmt])
          | update
          | delete
          | insert
          | createStatement // Delegates to subclass
          | (&(keyword("ALTER")) ~/
            alterView)
          | dropTableOrView
          | explainStatement
          | dialectSpecificStatement // Hook for dialect extensions
        )
    )

  def explainStatement[$ : P]: P[Stmt] = P(
    keyword("EXPLAIN") ~ select.map(s => sequala.schema.Explain(s).asInstanceOf[Stmt])
  )

  // Methods that can be overridden for dialect-specific behavior
  def createStatement[$ : P]: P[Stmt]
  def dialectSpecificStatement[$ : P]: P[Stmt]
  def dataType[$ : P]: P[DataType]

  // Shared logic with conditional branches
  def ifExists[$ : P]: P[Boolean] =
    if supportsIfExists then {
      P((keyword("IF") ~ keyword("EXISTS")).!.?.map(_ != None))
    } else {
      P(Pass).map(_ => false)
    }

  def orReplace[$ : P]: P[Boolean] = P((keyword("OR") ~ keyword("REPLACE")).!.?.map(_ != None))

  def alterView[$ : P]: P[Stmt] = P(
    (
      keyword("ALTER") ~
        keyword("VIEW") ~/
        identifier ~
        (
          (keyword("MATERIALIZE").!.map(_ => true))
            | (keyword("DROP") ~
              keyword("MATERIALIZE").!.map(_ => false))
        )
    ).map { case (name, add) =>
      sequala.schema.AlterView(name, sequala.schema.ast.Materialize(add)).asInstanceOf[Stmt]
    }
  )

  def dropTableOrView[$ : P]: P[Stmt] = P(
    (
      keyword("DROP") ~
        keyword("TABLE", "VIEW").!.map(_.toUpperCase) ~/
        ifExists ~
        identifier
    ).map {
      case ("TABLE", ifExists, name) =>
        import sequala.schema.{DropTable, CommonDropOptions}
        DropTable(name.name, ifExists, CommonDropOptions.empty).asInstanceOf[Stmt]
      case ("VIEW", ifExists, name) =>
        sequala.schema.DropView(name, ifExists).asInstanceOf[Stmt]
      case (_, _, _) =>
        throw new Exception("Internal Error")
    }
  )

  def createView[$ : P]: P[Stmt] = P(
    (
      keyword("CREATE") ~
        orReplace ~
        keyword("MATERIALIZED", "TEMPORARY").!.?.map {
          _.map(_.toUpperCase) match {
            case Some("MATERIALIZED") => (true, false)
            case Some("TEMPORARY") => (false, true)
            case _ => (false, false)
          }
        } ~
        keyword("VIEW") ~/
        identifier ~
        keyword("AS") ~/
        select
    ).map { case (orReplace, (materialized, temporary), name, query) =>
      sequala.schema.CreateView(name, orReplace, query, materialized, temporary).asInstanceOf[Stmt]
    }
  )

  // Parser for identifiers used as expressions without parentheses (e.g., CURRENT_TIMESTAMP, SYSDATE)
  def functionCallNoParens[$ : P]: P[Expression] = P(identifier.map { name =>
    // Use Identifier for bare identifiers (no parentheses)
    Identifier(name)
  })

  def columnAnnotation[$ : P]: P[ColumnAnnotation] = P(
    (
      keyword("PRIMARY") ~/
        keyword("KEY").map(_ => ColumnIsPrimaryKey())
    ) | (
      keyword("NOT") ~/
        keyword("NULL").map(_ => ColumnIsNotNullable())
    ) | (
      keyword("DEFAULT") ~/
        (
          ("(" ~ expression ~ ")")
            | cast // CAST expressions (including Oracle's format mask extension)
            | function // Function calls with parentheses
            | functionCallNoParens // Function calls without parentheses (CURRENT_TIMESTAMP, etc.)
            | primitive
        ).map(ColumnDefaultValue(_))
    )
  )

  def oneOrMoreAttributes[$ : P]: P[Seq[Name]] = P(
    ("(" ~/ identifier.rep(sep = comma, min = 1) ~ ")")
      | identifier.map(Seq(_))
  )

  def referentialAction[$ : P]: P[ReferentialAction] = P(
    (keyword("NO") ~ keyword("ACTION")).map(_ => NoAction)
      | keyword("RESTRICT").map(_ => Restrict)
      | keyword("CASCADE").map(_ => Cascade)
      | (keyword("SET") ~ keyword("NULL")).map(_ => SetNull)
      | (keyword("SET") ~ keyword("DEFAULT")).map(_ => SetDefault)
  )

  def onUpdateClause[$ : P]: P[ReferentialAction] = P(keyword("ON") ~ keyword("UPDATE") ~/ referentialAction)

  def onDeleteClause[$ : P]: P[ReferentialAction] = P(keyword("ON") ~ keyword("DELETE") ~/ referentialAction)

  def foreignKeyConstraint[$ : P]: P[TableForeignKey] = P(
    keyword("FOREIGN") ~/
      keyword("KEY") ~
      oneOrMoreAttributes ~
      keyword("REFERENCES") ~/
      qualifiedTableName ~
      oneOrMoreAttributes ~
      onUpdateClause.? ~
      onDeleteClause.?
  ).map { case (cols, refTable, refCols, onUpdate, onDelete) =>
    TableForeignKey(None, cols, refTable, refCols, onUpdate, onDelete)
  }

  def constraintDefinition[$ : P]: P[TableAnnotation] = P(
    keyword("CONSTRAINT") ~/
      identifier ~
      (
        (keyword("PRIMARY") ~ keyword("KEY") ~ oneOrMoreAttributes).map { cols => (name: Name) =>
          TablePrimaryKey(cols)
        }
          | (keyword("UNIQUE") ~ oneOrMoreAttributes).map { cols => (name: Name) => TableUnique(cols) }
          | (keyword("FOREIGN") ~ keyword("KEY") ~
            oneOrMoreAttributes ~
            keyword("REFERENCES") ~/
            qualifiedTableName ~
            oneOrMoreAttributes ~
            onUpdateClause.? ~
            onDeleteClause.?).map { case (cols, refTable, refCols, onUpdate, onDelete) =>
            (name: Name) => TableForeignKey(Some(name), cols, refTable, refCols, onUpdate, onDelete)
          }
          | (keyword("CHECK") ~ "(" ~/ expression ~ ")").map { expr => (name: Name) =>
            TableCheck(Some(name), expr)
          }
      )
  ).map { case (name, builder) => builder(name) }

  def tableField[$ : P]: P[Either[TableAnnotation, ColumnDefinition]] = P(
    (
      keyword("PRIMARY") ~/
        keyword("KEY") ~
        oneOrMoreAttributes.map(attrs => Left(TablePrimaryKey(attrs)))
    ) | (
      keyword("INDEX") ~/
        keyword("ON").? ~
        oneOrMoreAttributes.map(attrs => Left(TableIndexOn(attrs)))
    ) | (
      keyword("UNIQUE") ~/
        oneOrMoreAttributes.map(attrs => Left(TableUnique(attrs)))
    ) | (
      foreignKeyConstraint.map(Left(_))
    ) | (
      constraintDefinition.map(Left(_))
    ) | (
      (
        identifier ~/
          identifier ~
          ("(" ~
            primitive.rep(sep = ",") ~
            ")").?.map(_.getOrElse(Seq())) ~
          columnAnnotation.rep
      ).map { case (name, t, args, annotations) =>
        Right(ColumnDefinition(name, t, args, annotations))
      }
    )
  )

  def createTable[$ : P]: P[Stmt] = P(
    (
      keyword("CREATE") ~
        orReplace ~
        keyword("TABLE") ~/
        schemaQualifiedName ~
        (
          (keyword("AS") ~/ select).map(Left(_))
            | ("(" ~/
              tableField.rep(sep = comma) ~
              ")").map(Right(_))
        )
    ).map {
      case (orReplace, (schema, tableName), Left(query)) =>
        sequala.schema.CreateTableAs(Name(tableName), orReplace, query).asInstanceOf[Stmt]
      case (orReplace, (schema, tableName), Right(fields)) =>
        val columns = fields.collect { case Right(r) => r }
        val annotations = fields.collect { case Left(l) => l }
        val schemaCreateTable = SchemaConverter.convertCreateTable(tableName, schema, orReplace, columns, annotations)
        schemaCreateTable.asInstanceOf[Stmt]
    }
  )

  def createIndex[$ : P]: P[Stmt] = P(
    keyword("CREATE") ~
      keyword("UNIQUE").!.?.map(_.isDefined) ~
      keyword("INDEX") ~/
      identifier ~
      keyword("ON") ~/
      qualifiedTableName ~
      "(" ~/ identifier.rep(sep = comma, min = 1) ~ ")"
  ).map { case (unique, indexName, tableName, cols) =>
    import sequala.schema.{CreateIndex, IndexColumn}
    val indexCols = cols.map(name => IndexColumn(name.name))
    CreateIndex(indexName.name, tableName.name, indexCols, unique).asInstanceOf[Stmt]
  }

  def valueList[$ : P]: P[InsertValues] = P(
    (
      keyword("VALUES") ~/
        ("(" ~/ expressionList ~ ")").rep(sep = comma)
    ).map(sequala.schema.ast.ExplicitInsert(_))
  )

  def insert[$ : P]: P[Stmt] = P(
    (
      keyword("INSERT") ~/
        (
          keyword("OR") ~/
            keyword("REPLACE")
        ).!.?.map { case None => false; case _ => true } ~
        keyword("INTO") ~/
        identifier ~
        ("(" ~/
          identifier ~
          (comma ~/ identifier).rep ~
          ")").map(x => Seq(x._1) ++ x._2).? ~
        (
          (&(keyword("SELECT")) ~/ select.map(sequala.schema.ast.SelectInsert(_)))
            | (&(keyword("VALUES")) ~/ valueList)
        )
    ).map { case (orReplace, table, columns, values) =>
      sequala.schema.Insert(table, columns, values, orReplace).asInstanceOf[Stmt]
    }
  )

  def delete[$ : P]: P[Stmt] = P(
    (
      keyword("DELETE") ~/
        keyword("FROM") ~/
        identifier ~
        (
          keyword("WHERE") ~/
            expression
        ).?
    ).map { case (table, where) =>
      sequala.schema.Delete(table, where).asInstanceOf[Stmt]
    }
  )

  def update[$ : P]: P[Stmt] = P(
    (
      keyword("UPDATE") ~/
        identifier ~
        keyword("SET") ~/
        (
          identifier ~
            "=" ~/
            expression
        ).rep(sep = comma, min = 1) ~
        (
          StringInIgnoreCase("WHERE") ~/
            expression
        ).?
    ).map { case (table, set, where) =>
      sequala.schema.Update(table, set, where).asInstanceOf[Stmt]
    }
  )

  def alias[$ : P]: P[Name] =
    P(keyword("AS").? ~ identifier)

  def selectTarget[$ : P]: P[SelectTarget] = P(
    P("*").map(_ => SelectAll())
    // Dotted wildcard needs a lookahead since a single token isn't
    // enough to distinguish between `foo`.* and `foo` AS `bar`
      | (&(dottedWildcard) ~
        dottedWildcard.map(SelectTable(_)))
      | (expression ~ alias.?).map(x => SelectExpression(x._1, x._2))
  )

  def simpleFromElement[$ : P]: P[FromElement] = P(
    (("(" ~ select ~ ")" ~ alias).map(x => FromSelect(x._1, x._2)))
      | ((dottedPair ~ alias.?).map { case (schema, table, alias) =>
        FromTable(schema, table, alias)
      })
      | (("(" ~ fromElement ~ ")" ~ alias.?).map {
        case (from, None) => from
        case (from, Some(alias)) => from.withAlias(alias)
      })
  )

  def joinWith[$ : P]: P[Join.Type] = P(
    keyword("JOIN").map(Unit => Join.Type.Inner)
      | ((
        keyword("NATURAL").!.map(Unit => Join.Type.Natural)
          | keyword("INNER").map(Unit => Join.Type.Inner)
          | ((
            keyword("LEFT").map(Unit => Join.Type.LeftOuter)
              | keyword("RIGHT").map(Unit => Join.Type.RightOuter)
              | keyword("FULL").map(Unit => Join.Type.FullOuter)
          ).?.map(_.getOrElse(Join.Type.FullOuter)) ~/
            keyword("OUTER"))
      ) ~/ keyword("JOIN"))
  )

  def fromElement[$ : P]: P[FromElement] = P(
    (
      simpleFromElement ~ (
        &(joinWith) ~
          joinWith ~/
          simpleFromElement ~/
          (
            keyword("ON") ~/
              expression
          ).? ~
          alias.?
      ).rep
    ).map { case (lhs, rest) =>
      rest.foldLeft(lhs) { (lhs, next) =>
        val (t, rhs, onClause, alias) = next
        FromJoin(lhs, rhs, t, onClause.getOrElse(BooleanPrimitive(true)), alias)
      }
    }
  )

  def fromClause[$ : P] = P(
    keyword("FROM") ~/
      fromElement.rep(sep = comma, min = 1)
  )

  def whereClause[$ : P] = P(keyword("WHERE") ~/ expression)

  def groupByClause[$ : P] = P(
    keyword("GROUP") ~/
      keyword("BY") ~/
      expressionList
  )

  def havingClause[$ : P] = P(keyword("HAVING") ~ expression)

  def options[A](default: A, options: Map[String, A]): (Option[String] => A) =
    _.map(_.toUpperCase).map(options(_)).getOrElse(default)

  def ascOrDesc[$ : P] = P(keyword("ASC", "DESC").!.?.map {
    options(true, Map("ASC" -> true, "DESC" -> false))
  })

  def orderBy[$ : P] = P((expression ~ ascOrDesc).map(x => OrderBy(x._1, x._2)))

  def orderByClause[$ : P] = P(
    keyword("ORDER") ~/
      keyword("BY") ~/
      orderBy.rep(sep = comma, min = 1)
  )

  def limitClause[$ : P] = P(
    keyword("LIMIT") ~/
      integer
  )

  def offsetClause[$ : P] = P(
    keyword("OFFSET") ~/
      integer
  )

  def allOrDistinct[$ : P] = P(keyword("ALL", "DISTINCT").!.?.map {
    options(Union.Type.Distinct, Map("ALL" -> Union.Type.All, "DISTINCT" -> Union.Type.Distinct))
  })

  def unionClause[$ : P] = P(keyword("UNION") ~/ allOrDistinct ~/ parenthesizedSelect)

  def parenthesizedSelect[$ : P]: P[SelectBody] = P(
    (
      "(" ~/ select ~ ")" ~/ unionClause.?
    ).map {
      case (body, Some((unionType, unionBody))) => body.unionWith(unionType, unionBody)
      case (body, None) => body
    } | select
  )

  def select[$ : P]: P[SelectBody] = P(
    (
      keyword("SELECT") ~/
        keyword("DISTINCT").!.?.map(_ != None) ~/
        selectTarget.rep(sep = ",") ~
        fromClause.?.map(_.toSeq.flatten) ~
        whereClause.? ~
        groupByClause.? ~
        havingClause.? ~
        orderByClause.?.map(_.toSeq.flatten) ~
        limitClause.? ~
        offsetClause.? ~
        unionClause.?
    ).map { case (distinct, targets, froms, where, groupBy, having, orderBy, limit, offset, union) =>
      SelectBody(
        distinct = distinct,
        target = targets,
        from = froms,
        where = where,
        groupBy = groupBy,
        having = having,
        orderBy = orderBy,
        limit = limit,
        offset = offset,
        union = union
      )
    }
  )

  // Parse multiple statements with error recovery
  // Handle End separately to avoid infinite loops
  def allStatements[$ : P]: P[Seq[Stmt]] = P(Start ~ terminatedStatement.rep ~ End).map(_.toSeq)
}

trait SQLBaseObject {
  type Stmt <: Statement
  def name: String
  def apply(input: String): Parsed[Stmt]
  def apply(input: Reader): StreamParser[Stmt]

  // Abstract method to convert a statement to JSON (dialect-specific encoding)
  def statementToJson(stmt: Statement): Json

  // Abstract method to convert a statement to SQL (dialect-specific rendering)
  def statementToSql(stmt: Statement, pretty: Boolean = false): String

  // Abstract method to get statement terminator for this dialect
  protected def statementTerminator: String

  // Override this to specify all valid terminator characters (e.g., both ';' and '/')
  protected def statementTerminatorChars: Set[Char] = statementTerminator.toSet

  // Parse multiple statements with error recovery, tracking positions
  def parseAll(input: String): Seq[StatementParseResult] = {
    import sequala.common.statement.StatementParseResult
    import fastparse.*
    import fastparse.Parsed

    def parseStatementsSequentially(
      remaining: String,
      currentPos: Int,
      results: List[StatementParseResult]
    ): List[StatementParseResult] = {
      // Skip whitespace and comment lines at the start
      def skipWhitespaceAndComments(s: String): (String, Int) = {
        var pos = 0
        var skipped = 0
        while pos < s.length do {
          val char = s(pos)
          if char.isWhitespace then {
            pos += 1
            skipped += 1
          } else if pos + 1 < s.length && s(pos) == '-' && s(pos + 1) == '-' then {
            // Skip -- comment until newline
            while pos < s.length && s(pos) != '\n' do {
              pos += 1
              skipped += 1
            }
            // Skip the newline too
            if pos < s.length then {
              pos += 1
              skipped += 1
            }
          } else if pos + 1 < s.length && s(pos) == '/' && s(pos + 1) == '*' then {
            // Skip /* */ comment
            pos += 2
            skipped += 2
            while pos + 1 < s.length && !(s(pos) == '*' && s(pos + 1) == '/') do {
              pos += 1
              skipped += 1
            }
            if pos + 1 < s.length then {
              pos += 2
              skipped += 2
            }
          } else {
            // Found non-whitespace, non-comment content
            return (s.substring(pos), skipped)
          }
        }
        ("", skipped)
      }

      // Find the next statement terminator, skipping over string literals and comments
      def findNextTerminator(s: String, terminatorChars: Set[Char]): Int = {
        var pos = 0
        while pos < s.length do {
          val char = s(pos)
          if char == '\'' then {
            // Inside string literal - skip until closing quote (handle escaped quotes '')
            pos += 1
            while pos < s.length do {
              if s(pos) == '\'' then {
                if pos + 1 < s.length && s(pos + 1) == '\'' then {
                  pos += 2 // Skip escaped quote ''
                } else {
                  pos += 1 // End of string
                  return findNextTerminator(s.substring(pos), terminatorChars) match {
                    case -1 => -1
                    case n => pos + n
                  }
                }
              } else {
                pos += 1
              }
            }
            return -1 // Unclosed string
          } else if pos + 1 < s.length && char == '-' && s(pos + 1) == '-' then {
            // Skip -- comment until newline
            while pos < s.length && s(pos) != '\n' do pos += 1
          } else if pos + 1 < s.length && char == '/' && s(pos + 1) == '*' then {
            // Skip /* */ comment
            pos += 2
            while pos + 1 < s.length && !(s(pos) == '*' && s(pos + 1) == '/') do pos += 1
            if pos + 1 < s.length then pos += 2
          } else if terminatorChars.contains(char) then {
            return pos
          } else {
            pos += 1
          }
        }
        -1
      }

      val (trimmed, skipped) = skipWhitespaceAndComments(remaining)
      val statementStartPos = currentPos + skipped

      if trimmed.isEmpty then {
        return results.reverse
      }

      // Try to parse a single statement
      val parseResult = apply(trimmed)

      val (result, endPos) = parseResult match {
        case Parsed.Success(stmt, index) =>
          val parsedLength = index.toInt
          val statementEndPos = statementStartPos + parsedLength
          val parsedResult: Either[Unparseable, Stmt] = stmt match {
            case u: Unparseable => Left(u)
            case _ => Right(stmt.asInstanceOf[Stmt])
          }
          (parsedResult, statementEndPos)

        case Parsed.Failure(label, index, extra) =>
          // Parse failed - extract unparseable content
          val terminatorPos = findNextTerminator(trimmed, statementTerminatorChars)
          val unparseableEnd = if terminatorPos >= 0 then terminatorPos + 1 else trimmed.length

          val unparseableText = trimmed.substring(0, unparseableEnd).trim
          val unparseableEndPos = statementStartPos + unparseableEnd
          (Left(Unparseable(unparseableText)), unparseableEndPos)
      }

      val parseResultObj = StatementParseResult(result, statementStartPos, endPos)

      // Continue parsing from the end position
      val nextRemaining = if endPos < input.length then {
        input.substring(endPos)
      } else {
        ""
      }

      parseStatementsSequentially(nextRemaining, endPos, parseResultObj :: results)
    }

    parseStatementsSequentially(input, 0, Nil)
  }
}

object SQLBase {
  // Mirror of the reserved words used in anyKeyword macro (must be kept in sync)
  val reservedWordsSet: Set[String] = Set(
    "ADD",
    "ALL",
    "ALTER",
    "AND",
    "AS",
    "ASC",
    "BEGIN",
    "BETWEEN",
    "BY",
    "CASE",
    "CAST",
    "COLUMN",
    "COMMENT",
    "COMMIT",
    "CONSTRAINT",
    "CREATE",
    "DECLARE",
    "DEFAULT",
    "DEFINE",
    "DELETE",
    "DESC",
    "DISABLE",
    "DISTINCT",
    "DOUBLE",
    "DROP",
    "ELSE",
    "ELSIF",
    "ENABLE",
    "END",
    "EXCEPTION",
    "EXISTS",
    "EXPLAIN",
    "FALSE",
    "FOR",
    "FROM",
    "FULL",
    "GROUP",
    "GRANT",
    "HAVING",
    "IF",
    "IN",
    "INDEX",
    "INNER",
    "INSERT",
    "INTO",
    "INVISIBLE",
    "IS",
    "JOIN",
    "KEY",
    "LEFT",
    "LIMIT",
    "LOOP",
    "MATERIALIZE",
    "MATERIALIZED",
    "MODIFY",
    "NATURAL",
    "NOT",
    "NULL",
    "OFF",
    "OFFSET",
    "ON",
    "OR",
    "ORDER",
    "OVER",
    "OUTER",
    "PARTITION",
    "PRECISION",
    "PRIMARY",
    "PROMPT",
    "RENAME",
    "REPLACE",
    "REVERSE",
    "REVOKE",
    "RIGHT",
    "SELECT",
    "SET",
    "SYNONYM",
    "TABLE",
    "TEMPORARY",
    "THEN",
    "TO",
    "TRUE",
    "UNIQUE",
    "UPDATE",
    "UNION",
    "VALIDATE",
    "VALUES",
    "VIEW",
    "WHEN",
    "WHERE",
    "WHILE",
    "WITH",
    "FOREIGN",
    "REFERENCES",
    "CASCADE",
    "RESTRICT",
    "ACTION",
    "PURGE",
    "NOVALIDATE"
  )
}
