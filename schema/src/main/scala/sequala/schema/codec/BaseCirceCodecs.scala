package sequala.schema.codec

import cats.syntax.traverse.*
import io.circe.{Decoder, DecodingFailure, Encoder, Json}
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.syntax.*
import sequala.schema.*
import sequala.schema.ast.{
  AlterViewAction,
  Arithmetic,
  BlockComment,
  BooleanPrimitive,
  CaseWhenElse,
  Cast,
  Comparison,
  DoublePrimitive,
  Expression,
  FromElement,
  Function,
  Identifier,
  InExpression,
  InsertValues,
  IsNull,
  JDBCVar,
  Join,
  LineComment,
  LongPrimitive,
  Name,
  Not,
  NullPrimitive,
  OrderBy,
  PrimitiveValue,
  SelectBody,
  SelectTarget,
  SqlComment,
  StringPrimitive,
  Subquery,
  Union,
  WindowFunction,
  WithClause
}
import sequala.schema.ast.Column as AstColumn
import sequala.schema.statement.*

trait DialectCodecs:
  def statementEncoder: Encoder[Statement]
  def statementDecoder: Decoder[Statement]

abstract class BaseCirceCodecs extends DialectCodecs:
  self: DbDialect =>

  // Configuration for deriveConfiguredEncoder/Decoder
  given Configuration = Configuration.default.withDiscriminator("type").withDefaults

  // ==================== Common type encoders (non-parameterized) ====================

  // Name codec
  given Encoder[Name] = deriveEncoder[Name]
  given Decoder[Name] = deriveDecoder[Name]

  // SqlComment codecs
  given Encoder[LineComment] = Encoder.instance { lc =>
    Json.obj("type" -> Json.fromString("line"), "text" -> Json.fromString(lc.text))
  }
  given Encoder[BlockComment] = Encoder.instance { bc =>
    Json.obj("type" -> Json.fromString("block"), "text" -> Json.fromString(bc.text))
  }
  given Encoder[SqlComment] = Encoder.instance {
    case lc: LineComment => summon[Encoder[LineComment]].apply(lc)
    case bc: BlockComment => summon[Encoder[BlockComment]].apply(bc)
  }
  given Decoder[SqlComment] = Decoder.instance { cursor =>
    cursor.downField("type").as[String].flatMap {
      case "line" => cursor.downField("text").as[String].map(LineComment(_))
      case "block" => cursor.downField("text").as[String].map(BlockComment(_))
      case other => Left(DecodingFailure(s"Unknown SqlComment type: $other", cursor.history))
    }
  }

  // Enum codecs
  given arithmeticOpEncoder: Encoder[Arithmetic.Op] = Encoder.encodeString.contramap(_.toString)
  given arithmeticOpDecoder: Decoder[Arithmetic.Op] = Decoder.decodeString.emap { s =>
    try Right(Arithmetic.Op.valueOf(s))
    catch { case _: IllegalArgumentException => Left(s"Unknown Arithmetic.Op: $s") }
  }

  given comparisonOpEncoder: Encoder[Comparison.Op] = Encoder.encodeString.contramap(_.toString)
  given comparisonOpDecoder: Decoder[Comparison.Op] = Decoder.decodeString.emap { s =>
    try Right(Comparison.Op.valueOf(s))
    catch { case _: IllegalArgumentException => Left(s"Unknown Comparison.Op: $s") }
  }

  given joinTypeEncoder: Encoder[Join.Type] = Encoder.encodeString.contramap(_.toString)
  given joinTypeDecoder: Decoder[Join.Type] = Decoder.decodeString.emap { s =>
    try Right(Join.Type.valueOf(s))
    catch { case _: IllegalArgumentException => Left(s"Unknown Join.Type: $s") }
  }

  given unionTypeEncoder: Encoder[Union.Type] = Encoder.encodeString.contramap(_.toString)
  given unionTypeDecoder: Decoder[Union.Type] = Decoder.decodeString.emap { s =>
    try Right(Union.Type.valueOf(s))
    catch { case _: IllegalArgumentException => Left(s"Unknown Union.Type: $s") }
  }

  // Function codec
  given Encoder[Function] = deriveEncoder[Function]
  given Decoder[Function] = deriveDecoder[Function]

  // OrderBy codec
  given Encoder[OrderBy] = deriveEncoder[OrderBy]
  given Decoder[OrderBy] = deriveDecoder[OrderBy]

  // Expression codecs (recursive, requires custom implementation)
  given Encoder[Expression] = Encoder.instance {
    case lp: LongPrimitive =>
      Json.obj("type" -> Json.fromString("LongPrimitive"), "value" -> Json.fromLong(lp.v))
    case dp: DoublePrimitive =>
      Json.obj("type" -> Json.fromString("DoublePrimitive"), "value" -> Json.fromDoubleOrNull(dp.v))
    case sp: StringPrimitive =>
      Json.obj("type" -> Json.fromString("StringPrimitive"), "value" -> Json.fromString(sp.v))
    case bp: BooleanPrimitive =>
      Json.obj("type" -> Json.fromString("BooleanPrimitive"), "value" -> Json.fromBoolean(bp.v))
    case NullPrimitive() => Json.obj("type" -> Json.fromString("NullPrimitive"))
    case i: Identifier =>
      Json.obj("type" -> Json.fromString("Identifier"), "name" -> i.name.asJson)
    case c: AstColumn =>
      Json.obj("type" -> Json.fromString("Column"), "column" -> c.column.asJson, "table" -> c.table.asJson)
    case a: Arithmetic =>
      Json.obj(
        "type" -> Json.fromString("Arithmetic"),
        "lhs" -> a.lhs.asJson,
        "op" -> a.op.asJson,
        "rhs" -> a.rhs.asJson
      )
    case c: Comparison =>
      Json.obj(
        "type" -> Json.fromString("Comparison"),
        "lhs" -> c.lhs.asJson,
        "op" -> c.op.asJson,
        "rhs" -> c.rhs.asJson
      )
    case f: Function =>
      Json.obj(
        "type" -> Json.fromString("Function"),
        "name" -> f.name.asJson,
        "params" -> f.params.asJson,
        "distinct" -> Json.fromBoolean(f.distinct)
      )
    case wf: WindowFunction =>
      Json.obj(
        "type" -> Json.fromString("WindowFunction"),
        "function" -> wf.function.asJson,
        "partitionBy" -> wf.partitionBy.asJson,
        "orderBy" -> wf.orderBy.asJson
      )
    case JDBCVar() => Json.obj("type" -> Json.fromString("JDBCVar"))
    case sq: Subquery => Json.obj("type" -> Json.fromString("Subquery"), "query" -> sq.query.asJson)
    case cwe: CaseWhenElse =>
      Json.obj(
        "type" -> Json.fromString("CaseWhenElse"),
        "target" -> cwe.target.asJson,
        "cases" -> cwe.cases.map { case (e1, e2) => Json.obj("when" -> e1.asJson, "then" -> e2.asJson) }.asJson,
        "otherwise" -> cwe.otherwise.asJson
      )
    case in: IsNull => Json.obj("type" -> Json.fromString("IsNull"), "target" -> in.target.asJson)
    case n: Not => Json.obj("type" -> Json.fromString("Not"), "expr" -> n.expr.asJson)
    case cast: Cast =>
      Json.obj("type" -> Json.fromString("Cast"), "expression" -> cast.expression.asJson, "typeName" -> cast.t.asJson)
    case ie: InExpression =>
      Json.obj(
        "type" -> Json.fromString("InExpression"),
        "expression" -> ie.expression.asJson,
        "source" -> (ie.source match {
          case Left(exprs) => Json.obj("type" -> Json.fromString("Expressions"), "values" -> exprs.asJson)
          case Right(query) => Json.obj("type" -> Json.fromString("SelectBody"), "query" -> query.asJson)
        })
      )
  }

  given Decoder[Expression] = Decoder.instance { c =>
    c.get[String]("type").flatMap {
      case "LongPrimitive" => c.get[Long]("value").map(LongPrimitive.apply)
      case "DoublePrimitive" => c.get[Double]("value").map(DoublePrimitive.apply)
      case "StringPrimitive" => c.get[String]("value").map(StringPrimitive.apply)
      case "BooleanPrimitive" => c.get[Boolean]("value").map(BooleanPrimitive.apply)
      case "NullPrimitive" => Right(NullPrimitive())
      case "Identifier" => c.get[Name]("name").map(Identifier.apply)
      case "Column" =>
        for {
          col <- c.get[Name]("column")
          table <- c.get[Option[Name]]("table")
        } yield AstColumn(col, table)
      case "Arithmetic" =>
        for {
          lhs <- c.get[Expression]("lhs")
          op <- c.get[Arithmetic.Op]("op")
          rhs <- c.get[Expression]("rhs")
        } yield Arithmetic(lhs, op, rhs)
      case "Comparison" =>
        for {
          lhs <- c.get[Expression]("lhs")
          op <- c.get[Comparison.Op]("op")
          rhs <- c.get[Expression]("rhs")
        } yield Comparison(lhs, op, rhs)
      case "Function" =>
        for {
          name <- c.get[Name]("name")
          params <- c.get[Option[Seq[Expression]]]("params")
          distinct <- c.get[Boolean]("distinct")
        } yield Function(name, params, distinct)
      case "WindowFunction" =>
        for {
          func <- c.get[Function]("function")
          partitionBy <- c.get[Option[Seq[Expression]]]("partitionBy")
          orderBy <- c.get[Seq[OrderBy]]("orderBy")
        } yield WindowFunction(func, partitionBy, orderBy)
      case "JDBCVar" => Right(JDBCVar())
      case "Subquery" => c.get[SelectBody]("query").map(Subquery.apply)
      case "CaseWhenElse" =>
        for {
          target <- c.get[Option[Expression]]("target")
          cases <- c.get[Seq[Json]]("cases").flatMap { jsons =>
            jsons.traverse { json =>
              for {
                when <- json.hcursor.get[Expression]("when")
                thenExpr <- json.hcursor.get[Expression]("then")
              } yield (when, thenExpr)
            }
          }
          otherwise <- c.get[Expression]("otherwise")
        } yield CaseWhenElse(target, cases, otherwise)
      case "IsNull" => c.get[Expression]("target").map(IsNull.apply)
      case "Not" => c.get[Expression]("expr").map(Not.apply)
      case "Cast" =>
        for {
          expr <- c.get[Expression]("expression")
          typeName <- c.get[Name]("typeName")
        } yield Cast(expr, typeName)
      case "InExpression" =>
        for {
          expr <- c.get[Expression]("expression")
          source <- c.get[Json]("source").flatMap { json =>
            json.hcursor.get[String]("type").flatMap {
              case "Expressions" => json.hcursor.get[Seq[Expression]]("values").map(Left.apply)
              case "SelectBody" => json.hcursor.get[SelectBody]("query").map(Right.apply)
              case other => Left(DecodingFailure(s"Unknown InExpression source type: $other", json.hcursor.history))
            }
          }
        } yield InExpression(expr, source)
      case other => Left(DecodingFailure(s"Unknown Expression type: $other", c.history))
    }
  }

  // PrimitiveValue codec (subset of Expression)
  given Encoder[PrimitiveValue] = summon[Encoder[Expression]].contramap(identity[Expression])
  given Decoder[PrimitiveValue] = summon[Decoder[Expression]].emap {
    case pv: PrimitiveValue => Right(pv)
    case other => Left(s"Expected PrimitiveValue but got ${other.getClass.getSimpleName}")
  }

  // SelectTarget codecs
  given Encoder[SelectTarget] = deriveConfiguredEncoder[SelectTarget]
  given Decoder[SelectTarget] = deriveConfiguredDecoder[SelectTarget]

  // FromElement codecs
  given Encoder[FromElement] = deriveConfiguredEncoder[FromElement]
  given Decoder[FromElement] = deriveConfiguredDecoder[FromElement]

  // SelectBody codec
  given Encoder[SelectBody] = deriveEncoder[SelectBody]
  given Decoder[SelectBody] = deriveDecoder[SelectBody]

  // InsertValues codecs
  given Encoder[InsertValues] = deriveConfiguredEncoder[InsertValues]
  given Decoder[InsertValues] = deriveConfiguredDecoder[InsertValues]

  // Merge-related codecs
  given Encoder[MergeSource] = Encoder.instance {
    case MergeSourceTable(table, alias) =>
      Json.obj("type" -> Json.fromString("MergeSourceTable"), "table" -> table.asJson, "alias" -> alias.asJson)
    case MergeSourceSelect(query, alias) =>
      Json.obj(
        "type" -> Json.fromString("MergeSourceSelect"),
        "query" -> query.asJson,
        "alias" -> Json.fromString(alias)
      )
  }

  given Decoder[MergeSource] = Decoder.instance { c =>
    c.get[String]("type").flatMap {
      case "MergeSourceTable" =>
        for {
          table <- c.get[Name]("table")
          alias <- c.get[Option[String]]("alias")
        } yield MergeSourceTable(table, alias)
      case "MergeSourceSelect" =>
        for {
          query <- c.get[SelectBody]("query")
          alias <- c.get[String]("alias")
        } yield MergeSourceSelect(query, alias)
      case other => Left(DecodingFailure(s"Unknown MergeSource type: $other", c.history))
    }
  }

  given Encoder[MergeWhenMatched] = Encoder.instance { wm =>
    Json.obj("set" -> wm.set.asJson, "where" -> wm.where.asJson)
  }

  given Decoder[MergeWhenMatched] = Decoder.instance { c =>
    for {
      set <- c.get[Seq[(Name, Expression)]]("set")
      where <- c.get[Option[Expression]]("where")
    } yield MergeWhenMatched(set, where)
  }

  given Encoder[MergeWhenNotMatched] = Encoder.instance { wnm =>
    Json.obj("columns" -> wnm.columns.asJson, "values" -> wnm.values.asJson)
  }

  given Decoder[MergeWhenNotMatched] = Decoder.instance { c =>
    for {
      columns <- c.get[Seq[Name]]("columns")
      values <- c.get[Seq[Expression]]("values")
    } yield MergeWhenNotMatched(columns, values)
  }

  // WithClause codec
  given Encoder[WithClause] = deriveEncoder[WithClause]
  given Decoder[WithClause] = deriveDecoder[WithClause]

  // AlterViewAction codecs
  given Encoder[AlterViewAction] = deriveConfiguredEncoder[AlterViewAction]
  given Decoder[AlterViewAction] = deriveConfiguredDecoder[AlterViewAction]

  // ReferentialAction codecs
  given Encoder[ReferentialAction] = Encoder.encodeString.contramap {
    case NoAction => "NO ACTION"
    case Restrict => "RESTRICT"
    case Cascade => "CASCADE"
    case SetNull => "SET NULL"
    case SetDefault => "SET DEFAULT"
    case other => other.toSql
  }
  given Decoder[ReferentialAction] = Decoder.decodeString.emap {
    case "NO ACTION" => Right(NoAction)
    case "RESTRICT" => Right(Restrict)
    case "CASCADE" => Right(Cascade)
    case "SET NULL" => Right(SetNull)
    case "SET DEFAULT" => Right(SetDefault)
    case other => Left(s"Unknown ReferentialAction: $other")
  }

  // Constraint type codecs
  given Encoder[PrimaryKey] = deriveEncoder[PrimaryKey]
  given Decoder[PrimaryKey] = deriveDecoder[PrimaryKey]
  given Encoder[ForeignKey] = deriveEncoder[ForeignKey]
  given Decoder[ForeignKey] = deriveDecoder[ForeignKey]
  given Encoder[Unique] = deriveEncoder[Unique]
  given Decoder[Unique] = deriveDecoder[Unique]
  given Encoder[Check] = deriveEncoder[Check]
  given Decoder[Check] = deriveDecoder[Check]
  given Encoder[Index] = deriveEncoder[Index]
  given Decoder[Index] = deriveDecoder[Index]
  given Encoder[IndexColumn] = deriveEncoder[IndexColumn]
  given Decoder[IndexColumn] = deriveDecoder[IndexColumn]

  // TableConstraint codecs
  given Encoder[TableConstraint] = deriveConfiguredEncoder[TableConstraint]
  given Decoder[TableConstraint] = deriveConfiguredDecoder[TableConstraint]

  // CommonDropOptions codec
  given Encoder[CommonDropOptions] = deriveEncoder[CommonDropOptions]
  given Decoder[CommonDropOptions] = deriveDecoder[CommonDropOptions]

  // CommonCreateViewOptions codec
  given Encoder[CommonCreateViewOptions] = deriveEncoder[CommonCreateViewOptions]
  given Decoder[CommonCreateViewOptions] = deriveDecoder[CommonCreateViewOptions]

  // ColumnAnnotation encoder
  given Encoder[ColumnAnnotation] = Encoder.instance {
    case ColumnIsPrimaryKey() => Json.obj("type" -> Json.fromString("ColumnIsPrimaryKey"))
    case ColumnIsNotNullable() => Json.obj("type" -> Json.fromString("ColumnIsNotNullable"))
    case ColumnIsNullable() => Json.obj("type" -> Json.fromString("ColumnIsNullable"))
    case cd: ColumnDefaultValue => Json.obj("type" -> Json.fromString("ColumnDefaultValue"), "v" -> cd.v.asJson)
    case ColumnIsInvisible() => Json.obj("type" -> Json.fromString("ColumnIsInvisible"))
    case ColumnConstraintEnable() => Json.obj("type" -> Json.fromString("ColumnConstraintEnable"))
    case ColumnConstraintDisable() => Json.obj("type" -> Json.fromString("ColumnConstraintDisable"))
    case ColumnConstraintValidate() => Json.obj("type" -> Json.fromString("ColumnConstraintValidate"))
    case ColumnConstraintNovalidate() => Json.obj("type" -> Json.fromString("ColumnConstraintNovalidate"))
  }

  // TableAnnotation encoder
  given Encoder[TableAnnotation] = Encoder.instance {
    case tpk: TablePrimaryKey =>
      Json.obj("type" -> Json.fromString("TablePrimaryKey"), "columns" -> tpk.columns.asJson)
    case tio: TableIndexOn =>
      Json.obj("type" -> Json.fromString("TableIndexOn"), "columns" -> tio.columns.asJson)
    case tu: TableUnique =>
      Json.obj("type" -> Json.fromString("TableUnique"), "columns" -> tu.columns.asJson)
    case tfk: TableForeignKey =>
      Json.obj(
        "type" -> Json.fromString("TableForeignKey"),
        "name" -> tfk.name.asJson,
        "columns" -> tfk.columns.asJson,
        "refTable" -> tfk.refTable.asJson,
        "refColumns" -> tfk.refColumns.asJson,
        "onUpdate" -> tfk.onUpdate.asJson,
        "onDelete" -> tfk.onDelete.asJson
      )
  }

  // ColumnDefinition encoder
  given Encoder[ColumnDefinition] = Encoder.instance { cd =>
    Json.obj(
      "name" -> cd.name.asJson,
      "t" -> cd.t.asJson,
      "args" -> cd.args.asJson,
      "annotations" -> cd.annotations.asJson
    )
  }

  // Custom codec for (Name, Expression) tuples
  given Encoder[(Name, Expression)] = Encoder.instance { case (name, expr) =>
    Json.obj("name" -> name.asJson, "expression" -> expr.asJson)
  }
  given Decoder[(Name, Expression)] = Decoder.instance { c =>
    for {
      name <- c.get[Name]("name")
      expr <- c.get[Expression]("expression")
    } yield (name, expr)
  }

  // ==================== Abstract givens that subclasses must provide ====================
  given dataTypeEncoder: Encoder[DataType]
  given dataTypeDecoder: Decoder[DataType]
  given columnOptionsEncoder: Encoder[ColumnOptions]
  given columnOptionsDecoder: Decoder[ColumnOptions]
  given tableOptionsEncoder: Encoder[TableOptions]
  given tableOptionsDecoder: Decoder[TableOptions]
  given dropOptionsEncoder: Encoder[DropOptions]
  given dropOptionsDecoder: Decoder[DropOptions]
  given createViewOptionsEncoder: Encoder[CreateViewOptions]
  given createViewOptionsDecoder: Decoder[CreateViewOptions]
  given dropViewOptionsEncoder: Encoder[DropViewOptions]
  given dropViewOptionsDecoder: Decoder[DropViewOptions]
  given indexOptionsEncoder: Encoder[IndexOptions]
  given indexOptionsDecoder: Decoder[IndexOptions]
  given explainOptionsEncoder: Encoder[ExplainOptions]
  given explainOptionsDecoder: Decoder[ExplainOptions]

  // Conditional givens for parameterized types

  given columnEncoder(using
    dtEnc: Encoder[DataType],
    coEnc: Encoder[ColumnOptions]
  ): Encoder[Column[DataType, ColumnOptions]] = Encoder.instance { col =>
    val base = Json.obj(
      "name" -> Json.fromString(col.name),
      "dataType" -> col.dataType.asJson(using dtEnc),
      "nullable" -> Json.fromBoolean(col.nullable),
      "default" -> col.default.asJson,
      "options" -> col.options.asJson(using coEnc),
      "comment" -> col.comment.asJson
    )
    // Only include sourceComment if non-empty (keeps JSON cleaner for jq)
    if col.sourceComment.nonEmpty then base.deepMerge(Json.obj("sourceComment" -> col.sourceComment.asJson))
    else base
  }

  given columnDecoder(using
    dtDec: Decoder[DataType],
    coDec: Decoder[ColumnOptions]
  ): Decoder[Column[DataType, ColumnOptions]] = Decoder.instance { c =>
    for {
      name <- c.get[String]("name")
      dataType <- c.get[DataType]("dataType")(using dtDec)
      nullable <- c.getOrElse[Boolean]("nullable")(true)
      default <- c.get[Option[String]]("default")
      options <- c.get[ColumnOptions]("options")(using coDec)
      comment <- c.get[Option[String]]("comment")
      sourceComment <- c.getOrElse[Seq[SqlComment]]("sourceComment")(Seq.empty)
    } yield Column(name, dataType, nullable, default, options, comment, sourceComment)
  }

  given tableEncoder(using
    dtEnc: Encoder[DataType],
    coEnc: Encoder[ColumnOptions],
    toEnc: Encoder[TableOptions]
  ): Encoder[Table[DataType, ColumnOptions, TableOptions]] =
    Encoder.instance { table =>
      val base = Json.obj(
        "name" -> Json.fromString(table.name),
        "schema" -> table.schema.asJson,
        "columns" -> table.columns.asJson(using Encoder.encodeSeq(using columnEncoder)),
        "primaryKey" -> table.primaryKey.asJson,
        "indexes" -> table.indexes.asJson,
        "foreignKeys" -> table.foreignKeys.asJson,
        "checks" -> table.checks.asJson,
        "uniques" -> table.uniques.asJson,
        "options" -> table.options.asJson(using toEnc),
        "comment" -> table.comment.asJson
      )
      // Only include sourceComment if non-empty (keeps JSON cleaner for jq)
      if table.sourceComment.nonEmpty then base.deepMerge(Json.obj("sourceComment" -> table.sourceComment.asJson))
      else base
    }

  given tableDecoder(using
    dtDec: Decoder[DataType],
    coDec: Decoder[ColumnOptions],
    toDec: Decoder[TableOptions]
  ): Decoder[Table[DataType, ColumnOptions, TableOptions]] =
    Decoder.instance { c =>
      for {
        name <- c.get[String]("name")
        schema <- c.getOrElse[Option[String]]("schema")(None)
        columns <- c.get[Seq[Column[DataType, ColumnOptions]]]("columns")(using Decoder.decodeSeq(using columnDecoder))
        primaryKey <- c.get[Option[PrimaryKey]]("primaryKey")
        indexes <- c.getOrElse[Seq[Index]]("indexes")(Seq.empty)
        foreignKeys <- c.getOrElse[Seq[ForeignKey]]("foreignKeys")(Seq.empty)
        checks <- c.getOrElse[Seq[Check]]("checks")(Seq.empty)
        uniques <- c.getOrElse[Seq[Unique]]("uniques")(Seq.empty)
        options <- c.get[TableOptions]("options")(using toDec)
        comment <- c.get[Option[String]]("comment")
        sourceComment <- c.getOrElse[Seq[SqlComment]]("sourceComment")(Seq.empty)
      } yield Table(
        name,
        columns,
        primaryKey,
        indexes,
        foreignKeys,
        checks,
        uniques,
        options,
        comment,
        schema,
        sourceComment
      )
    }

  given createTableEncoder(using
    tableEnc: Encoder[Table[DataType, ColumnOptions, TableOptions]]
  ): Encoder[CreateTable[DataType, ColumnOptions, TableOptions]] =
    Encoder.instance { ct =>
      Json.obj(
        "type" -> Json.fromString("CreateTable"),
        "table" -> ct.table.asJson(using tableEnc),
        "orReplace" -> Json.fromBoolean(ct.orReplace),
        "ifNotExists" -> Json.fromBoolean(ct.ifNotExists)
      )
    }

  given createTableDecoder(using
    tableDec: Decoder[Table[DataType, ColumnOptions, TableOptions]]
  ): Decoder[CreateTable[DataType, ColumnOptions, TableOptions]] =
    Decoder.instance { c =>
      for {
        table <- c.get[Table[DataType, ColumnOptions, TableOptions]]("table")(using tableDec)
        orReplace <- c.getOrElse[Boolean]("orReplace")(false)
        ifNotExists <- c.getOrElse[Boolean]("ifNotExists")(false)
      } yield CreateTable(table, orReplace, ifNotExists)
    }

  given dropTableEncoder(using doEnc: Encoder[DropOptions]): Encoder[DropTable[DropOptions]] = Encoder.instance { dt =>
    Json.obj(
      "type" -> Json.fromString("DropTable"),
      "tableName" -> Json.fromString(dt.tableName),
      "ifExists" -> Json.fromBoolean(dt.ifExists),
      "options" -> dt.options.asJson(using doEnc)
    )
  }

  given dropTableDecoder(using doDec: Decoder[DropOptions]): Decoder[DropTable[DropOptions]] = Decoder.instance { c =>
    for {
      tableName <- c.get[String]("tableName")
      ifExists <- c.getOrElse[Boolean]("ifExists")(false)
      options <- c.get[DropOptions]("options")(using doDec)
    } yield DropTable(tableName, ifExists, options)
  }

  // AlterTableAction encoders - base actions that are common across dialects
  given addColumnEncoder(using
    colEnc: Encoder[Column[DataType, ColumnOptions]]
  ): Encoder[AddColumn[DataType, ColumnOptions]] = Encoder.instance { ac =>
    Json.obj("type" -> Json.fromString("AddColumn"), "column" -> ac.column.asJson(using colEnc))
  }

  given addColumnDecoder(using
    colDec: Decoder[Column[DataType, ColumnOptions]]
  ): Decoder[AddColumn[DataType, ColumnOptions]] = Decoder.instance { c =>
    c.get[Column[DataType, ColumnOptions]]("column")(using colDec).map(col => AddColumn(col))
  }

  given dropColumnEncoder: Encoder[DropColumn[DataType, ColumnOptions]] = Encoder.instance { dc =>
    Json.obj(
      "type" -> Json.fromString("DropColumn"),
      "columnName" -> Json.fromString(dc.columnName),
      "cascade" -> Json.fromBoolean(dc.cascade)
    )
  }

  given dropColumnDecoder: Decoder[DropColumn[DataType, ColumnOptions]] = Decoder.instance { c =>
    for {
      columnName <- c.get[String]("columnName")
      cascade <- c.getOrElse[Boolean]("cascade")(false)
    } yield DropColumn(columnName, cascade)
  }

  given modifyColumnEncoder(using
    colEnc: Encoder[Column[DataType, ColumnOptions]]
  ): Encoder[ModifyColumn[DataType, ColumnOptions]] = Encoder.instance { mc =>
    Json.obj("type" -> Json.fromString("ModifyColumn"), "column" -> mc.column.asJson(using colEnc))
  }

  given modifyColumnDecoder(using
    colDec: Decoder[Column[DataType, ColumnOptions]]
  ): Decoder[ModifyColumn[DataType, ColumnOptions]] = Decoder.instance { c =>
    c.get[Column[DataType, ColumnOptions]]("column")(using colDec).map(col => ModifyColumn(col))
  }

  given renameColumnEncoder: Encoder[RenameColumn[DataType, ColumnOptions]] = Encoder.instance { rc =>
    Json.obj(
      "type" -> Json.fromString("RenameColumn"),
      "oldName" -> Json.fromString(rc.oldName),
      "newName" -> Json.fromString(rc.newName)
    )
  }

  given renameColumnDecoder: Decoder[RenameColumn[DataType, ColumnOptions]] = Decoder.instance { c =>
    for {
      oldName <- c.get[String]("oldName")
      newName <- c.get[String]("newName")
    } yield RenameColumn(oldName, newName)
  }

  given addConstraintEncoder: Encoder[AddConstraint[DataType, ColumnOptions]] = Encoder.instance { ac =>
    Json.obj("type" -> Json.fromString("AddConstraint"), "constraint" -> ac.constraint.asJson)
  }

  given addConstraintDecoder: Decoder[AddConstraint[DataType, ColumnOptions]] = Decoder.instance { c =>
    c.get[TableConstraint]("constraint").map(tc => AddConstraint(tc))
  }

  given dropConstraintEncoder: Encoder[DropConstraint[DataType, ColumnOptions]] = Encoder.instance { dc =>
    Json.obj(
      "type" -> Json.fromString("DropConstraint"),
      "constraintName" -> Json.fromString(dc.constraintName),
      "cascade" -> Json.fromBoolean(dc.cascade)
    )
  }

  given dropConstraintDecoder: Decoder[DropConstraint[DataType, ColumnOptions]] = Decoder.instance { c =>
    for {
      constraintName <- c.get[String]("constraintName")
      cascade <- c.getOrElse[Boolean]("cascade")(false)
    } yield DropConstraint(constraintName, cascade)
  }

  // AlterTable encoder - uses the concrete type members
  given alterTableEncoder(using
    ataEnc: Encoder[AlterTableAction[DataType, ColumnOptions]]
  ): Encoder[AlterTable[DataType, ColumnOptions, TableOptions, AlterTableAction[DataType, ColumnOptions]]] =
    Encoder.instance { at =>
      Json.obj(
        "type" -> Json.fromString("AlterTable"),
        "tableName" -> Json.fromString(at.tableName),
        "actions" -> at.actions.asJson(using Encoder.encodeSeq(using ataEnc))
      )
    }

  // AlterTableAction encoder - pattern match on known action types
  given alterTableActionEncoder(using
    colEnc: Encoder[Column[DataType, ColumnOptions]]
  ): Encoder[AlterTableAction[DataType, ColumnOptions]] =
    Encoder.instance {
      case ac: AddColumn[DataType, ColumnOptions] @unchecked => ac.asJson(using addColumnEncoder)
      case dc: DropColumn[DataType, ColumnOptions] @unchecked => dc.asJson(using dropColumnEncoder)
      case mc: ModifyColumn[DataType, ColumnOptions] @unchecked => mc.asJson(using modifyColumnEncoder)
      case rc: RenameColumn[DataType, ColumnOptions] @unchecked => rc.asJson(using renameColumnEncoder)
      case ac: AddConstraint[DataType, ColumnOptions] @unchecked => ac.asJson(using addConstraintEncoder)
      case dc: DropConstraint[DataType, ColumnOptions] @unchecked => dc.asJson(using dropConstraintEncoder)
      case other => encodeDialectSpecificAction(other)
    }

  // Hook for dialect-specific alter table actions
  protected def encodeDialectSpecificAction(action: AlterTableAction[DataType, ColumnOptions]): Json =
    Json.obj("type" -> Json.fromString(action.getClass.getSimpleName))

  // Statement encoder - uses casts for parameterized types (safe due to erasure)
  given dialectStatementEncoder: Encoder[Statement] = Encoder.instance { stmt =>
    stmt match
      // Non-parameterized types - encode directly
      case EmptyStatement() => Json.obj("type" -> Json.fromString("EmptyStatement"))
      case u: Unparseable =>
        Json.obj("type" -> Json.fromString("Unparseable"), "content" -> Json.fromString(u.content))
      case s: Select =>
        Json.obj("type" -> Json.fromString("Select"), "body" -> s.body.asJson, "withClause" -> s.withClause.asJson)
      case u: Update =>
        Json.obj(
          "type" -> Json.fromString("Update"),
          "table" -> u.table.asJson,
          "set" -> u.set.map { case (n, e) => Json.obj("name" -> n.asJson, "expression" -> e.asJson) }.asJson,
          "where" -> u.where.asJson
        )
      case d: Delete =>
        Json.obj("type" -> Json.fromString("Delete"), "table" -> d.table.asJson, "where" -> d.where.asJson)
      case i: Insert =>
        Json.obj(
          "type" -> Json.fromString("Insert"),
          "table" -> i.table.asJson,
          "columns" -> i.columns.asJson,
          "values" -> i.values.asJson,
          "orReplace" -> Json.fromBoolean(i.orReplace)
        )
      case m: Merge =>
        Json.obj(
          "type" -> Json.fromString("Merge"),
          "intoTable" -> m.intoTable.asJson,
          "intoAlias" -> m.intoAlias.asJson,
          "usingSource" -> m.usingSource.asJson,
          "onCondition" -> m.onCondition.asJson,
          "whenMatched" -> m.whenMatched.asJson,
          "whenNotMatched" -> m.whenNotMatched.asJson
        )
      case cta: CreateTableAs =>
        Json.obj(
          "type" -> Json.fromString("CreateTableAs"),
          "name" -> cta.name.asJson,
          "orReplace" -> Json.fromBoolean(cta.orReplace),
          "query" -> cta.query.asJson
        )
      case cv: CreateView[?] =>
        Json.obj(
          "type" -> Json.fromString("CreateView"),
          "name" -> cv.name.asJson,
          "orReplace" -> Json.fromBoolean(cv.orReplace),
          "query" -> cv.query.asJson,
          "options" -> cv.options.asInstanceOf[CreateViewOptions].asJson(using createViewOptionsEncoder)
        )
      case av: AlterView =>
        Json.obj("type" -> Json.fromString("AlterView"), "name" -> av.name.asJson, "action" -> av.action.asJson)
      case dv: DropView[?] =>
        Json.obj(
          "type" -> Json.fromString("DropView"),
          "name" -> dv.name.asJson,
          "ifExists" -> Json.fromBoolean(dv.ifExists),
          "options" -> dv.options.asInstanceOf[DropViewOptions].asJson(using dropViewOptionsEncoder)
        )
      case ci: CreateIndex[?] =>
        Json.obj(
          "type" -> Json.fromString("CreateIndex"),
          "name" -> Json.fromString(ci.name),
          "tableName" -> Json.fromString(ci.tableName),
          "columns" -> ci.columns.asJson,
          "unique" -> Json.fromBoolean(ci.unique),
          "ifNotExists" -> Json.fromBoolean(ci.ifNotExists),
          "options" -> ci.options.asInstanceOf[IndexOptions].asJson(using indexOptionsEncoder)
        )
      case di: DropIndex =>
        Json.obj(
          "type" -> Json.fromString("DropIndex"),
          "name" -> Json.fromString(di.name),
          "ifExists" -> Json.fromBoolean(di.ifExists),
          "cascade" -> Json.fromBoolean(di.cascade)
        )
      case e: Explain[?] =>
        Json.obj(
          "type" -> Json.fromString("Explain"),
          "query" -> e.query.asJson,
          "options" -> e.options.asInstanceOf[ExplainOptions].asJson(using explainOptionsEncoder)
        )
      case stc: SetTableComment =>
        Json.obj(
          "type" -> Json.fromString("SetTableComment"),
          "tableName" -> Json.fromString(stc.tableName),
          "comment" -> stc.comment.asJson
        )
      case scc: SetColumnComment =>
        Json.obj(
          "type" -> Json.fromString("SetColumnComment"),
          "tableName" -> Json.fromString(scc.tableName),
          "columnName" -> Json.fromString(scc.columnName),
          "comment" -> scc.comment.asJson
        )
      // Parameterized types - cast and use type-aware encoders
      case ct: CreateTable[?, ?, ?] =>
        ct.asInstanceOf[CreateTable[DataType, ColumnOptions, TableOptions]].asJson(using createTableEncoder)
      case dt: DropTable[?] =>
        dt.asInstanceOf[DropTable[DropOptions]].asJson(using dropTableEncoder)
      case at: AlterTable[?, ?, ?, ?] =>
        at.asInstanceOf[AlterTable[DataType, ColumnOptions, TableOptions, AlterTableAction[DataType, ColumnOptions]]]
          .asJson(using alterTableEncoder)
      // Dialect-specific statements - delegate to subclass hook
      case other => encodeDialectSpecific(other)
  }

  // Hook for dialect-specific statements - override in subclasses
  protected def encodeDialectSpecific(stmt: Statement): Json =
    Json.obj("type" -> Json.fromString(stmt.getClass.getSimpleName), "unsupported" -> Json.fromBoolean(true))

  // Statement decoder - uses type discriminator to determine case class
  given dialectStatementDecoder: Decoder[Statement] = Decoder.instance { c =>
    c.get[String]("type").flatMap {
      case "EmptyStatement" => Right(EmptyStatement())
      case "Unparseable" => c.get[String]("content").map(Unparseable.apply)
      case "Select" =>
        for {
          body <- c.get[SelectBody]("body")
          withClause <- c.getOrElse[Seq[WithClause]]("withClause")(Seq.empty)
        } yield Select(body, withClause)
      case "Update" =>
        for {
          table <- c.get[Name]("table")
          set <- c.get[Seq[Json]]("set").flatMap { jsons =>
            jsons.traverse { json =>
              for {
                name <- json.hcursor.get[Name]("name")
                expr <- json.hcursor.get[Expression]("expression")
              } yield (name, expr)
            }
          }
          where <- c.get[Option[Expression]]("where")
        } yield Update(table, set, where)
      case "Delete" =>
        for {
          table <- c.get[Name]("table")
          where <- c.get[Option[Expression]]("where")
        } yield Delete(table, where)
      case "Insert" =>
        for {
          table <- c.get[Name]("table")
          columns <- c.get[Option[Seq[Name]]]("columns")
          values <- c.get[InsertValues]("values")
          orReplace <- c.getOrElse[Boolean]("orReplace")(false)
        } yield Insert(table, columns, values, orReplace)
      case "Merge" =>
        for {
          intoTable <- c.get[Name]("intoTable")
          intoAlias <- c.get[Option[String]]("intoAlias")
          usingSource <- c.get[MergeSource]("usingSource")
          onCondition <- c.get[Expression]("onCondition")
          whenMatched <- c.get[Option[MergeWhenMatched]]("whenMatched")
          whenNotMatched <- c.get[Option[MergeWhenNotMatched]]("whenNotMatched")
        } yield Merge(intoTable, intoAlias, usingSource, onCondition, whenMatched, whenNotMatched)
      case "CreateTableAs" =>
        for {
          name <- c.get[Name]("name")
          orReplace <- c.getOrElse[Boolean]("orReplace")(false)
          query <- c.get[SelectBody]("query")
        } yield CreateTableAs(name, orReplace, query)
      case "CreateView" =>
        for {
          name <- c.get[Name]("name")
          orReplace <- c.getOrElse[Boolean]("orReplace")(false)
          query <- c.get[SelectBody]("query")
          options <- c.get[CreateViewOptions]("options")(using createViewOptionsDecoder)
        } yield CreateView(name, orReplace, query, options)
      case "AlterView" =>
        for {
          name <- c.get[Name]("name")
          action <- c.get[AlterViewAction]("action")
        } yield AlterView(name, action)
      case "DropView" =>
        for {
          name <- c.get[Name]("name")
          ifExists <- c.getOrElse[Boolean]("ifExists")(false)
          options <- c.get[DropViewOptions]("options")(using dropViewOptionsDecoder)
        } yield DropView(name, ifExists, options)
      case "CreateIndex" =>
        for {
          name <- c.get[String]("name")
          tableName <- c.get[String]("tableName")
          columns <- c.get[Seq[IndexColumn]]("columns")
          unique <- c.getOrElse[Boolean]("unique")(false)
          ifNotExists <- c.getOrElse[Boolean]("ifNotExists")(false)
          options <- c.get[IndexOptions]("options")(using indexOptionsDecoder)
        } yield CreateIndex(name, tableName, columns, unique, ifNotExists, options)
      case "DropIndex" =>
        for {
          name <- c.get[String]("name")
          ifExists <- c.getOrElse[Boolean]("ifExists")(false)
          cascade <- c.getOrElse[Boolean]("cascade")(false)
        } yield DropIndex(name, ifExists, cascade)
      case "Explain" =>
        for {
          query <- c.get[SelectBody]("query")
          options <- c.getOrElse[ExplainOptions]("options")(NoExplainOptions.asInstanceOf[ExplainOptions])(using
            explainOptionsDecoder
          )
        } yield Explain(query, options)
      case "SetTableComment" =>
        for {
          tableName <- c.get[String]("tableName")
          comment <- c.get[Option[String]]("comment")
        } yield SetTableComment(tableName, comment)
      case "SetColumnComment" =>
        for {
          tableName <- c.get[String]("tableName")
          columnName <- c.get[String]("columnName")
          comment <- c.get[Option[String]]("comment")
        } yield SetColumnComment(tableName, columnName, comment)
      case "CreateTable" =>
        createTableDecoder(c).map(identity[Statement])
      case "DropTable" =>
        dropTableDecoder(c).map(identity[Statement])
      case "AlterTable" =>
        decodeAlterTable(c).map(identity[Statement])
      case other =>
        decodeDialectSpecific(c, other)
    }
  }

  // Hook for dialect-specific statement decoding - override in subclasses
  protected def decodeDialectSpecific(c: io.circe.HCursor, typeName: String): Decoder.Result[Statement] =
    Left(DecodingFailure(s"Unknown statement type: $typeName", c.history))

  // AlterTable decoder - needs to be a method since it depends on type members
  protected def decodeAlterTable(
    c: io.circe.HCursor
  ): Decoder.Result[AlterTable[DataType, ColumnOptions, TableOptions, AlterTableAction[DataType, ColumnOptions]]] =
    for {
      tableName <- c.get[String]("tableName")
      actions <- c.get[Seq[AlterTableAction[DataType, ColumnOptions]]]("actions")(using
        Decoder.decodeSeq(using alterTableActionDecoder)
      )
    } yield AlterTable(tableName, actions)

  // AlterTableAction decoder
  given alterTableActionDecoder(using
    colDec: Decoder[Column[DataType, ColumnOptions]]
  ): Decoder[AlterTableAction[DataType, ColumnOptions]] =
    Decoder.instance { c =>
      c.get[String]("type").flatMap {
        case "AddColumn" => addColumnDecoder(c)
        case "DropColumn" => dropColumnDecoder(c)
        case "ModifyColumn" => modifyColumnDecoder(c)
        case "RenameColumn" => renameColumnDecoder(c)
        case "AddConstraint" => addConstraintDecoder(c)
        case "DropConstraint" => dropConstraintDecoder(c)
        case other => decodeDialectSpecificAction(c, other)
      }
    }

  // Hook for dialect-specific alter table action decoding
  protected def decodeDialectSpecificAction(
    c: io.circe.HCursor,
    typeName: String
  ): Decoder.Result[AlterTableAction[DataType, ColumnOptions]] =
    Left(DecodingFailure(s"Unknown AlterTableAction type: $typeName", c.history))

  // DialectCodecs trait implementation
  def statementEncoder: Encoder[Statement] = dialectStatementEncoder
  def statementDecoder: Decoder[Statement] = dialectStatementDecoder
