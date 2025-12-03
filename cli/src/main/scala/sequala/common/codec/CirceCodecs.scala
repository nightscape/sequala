package sequala.common.codec

import io.circe.{Decoder, DecodingFailure, Encoder, HCursor}
import io.circe.{Json => CirceJson}
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.syntax._
import cats.syntax.traverse._
import sequala.common.Name
import sequala.schema.{
  CommonDataType,
  DataType,
  Decimal,
  DoublePrecision,
  Numeric,
  Real,
  SmallInt,
  SqlBigInt,
  SqlBlob,
  SqlBoolean,
  SqlChar,
  SqlClob,
  SqlDate,
  SqlInteger,
  SqlText,
  SqlTime,
  SqlTimestamp,
  VarChar
}
import sequala.schema.oracle.{
  BinaryDouble,
  BinaryFloat,
  Bytes,
  Chars,
  IntervalDayToSecond,
  IntervalYearToMonth,
  LongRaw,
  NChar,
  NVarchar2,
  Number,
  OracleChar,
  OracleDataType,
  OracleLong,
  OracleRowid,
  OracleSpecificDataType,
  Raw,
  SizeSemantics,
  URowid,
  Varchar2,
  XMLType
}
import sequala.schema.postgres.{
  BigSerial => PostgresBigSerial,
  Bit,
  BitVarying,
  Bytea,
  Cidr => PostgresCidr,
  Inet => PostgresInet,
  Json => PostgresJson,
  Jsonb => PostgresJsonb,
  MacAddr => PostgresMacAddr,
  MacAddr8,
  Money,
  PgArray,
  PgBox,
  PgCircle,
  PgLine,
  PgPath,
  PgPoint,
  PgPolygon,
  PostgresDataType,
  PostgresSpecificDataType,
  Serial => PostgresSerial,
  SmallSerial => PostgresSmallSerial,
  TsQuery,
  TsVector,
  Uuid => PostgresUuid
}
import sequala.common.expression._
import sequala.common.statement._
import sequala.common.select._
import sequala.common.alter._
import sequala.oracle._
import sequala.schema.{
  AddColumn,
  AddConstraint,
  AlterTableAction,
  Cascade,
  Check,
  CheckConstraint,
  Column => SchemaColumn,
  DropColumn,
  DropConstraint,
  ForeignKey,
  ForeignKeyConstraint,
  ModifyColumn,
  NoAction,
  NoColumnOptions,
  PrimaryKey,
  PrimaryKeyConstraint,
  ReferentialAction,
  RenameColumn,
  Restrict,
  SetDefault,
  SetNull,
  TableConstraint,
  Unique,
  UniqueConstraint
}

/** Circe codecs for all parser result datatypes */
object CirceCodecs {
  // Configuration for sealed trait/class hierarchies with "type" discriminator
  implicit val config: Configuration = Configuration.default.withDiscriminator("type")

  // Name codec
  implicit val nameEncoder: Encoder[Name] = deriveEncoder[Name]
  implicit val nameDecoder: Decoder[Name] = deriveDecoder[Name]

  // Function codec (needs explicit codec due to generic derivation issues)
  implicit val functionEncoder: Encoder[Function] = deriveEncoder[Function]
  implicit val functionDecoder: Decoder[Function] = deriveDecoder[Function]

  // SizeSemantics codec for Oracle types
  implicit val sizeSemanticsEncoder: Encoder[SizeSemantics] = Encoder.encodeString.contramap {
    case Bytes => "BYTE"
    case Chars => "CHAR"
  }
  implicit val sizeSemanticsDecoder: Decoder[SizeSemantics] = Decoder.decodeString.emap {
    case "BYTE" => Right(Bytes)
    case "CHAR" => Right(Chars)
    case other => Left(s"Unknown SizeSemantics: $other")
  }

  // DataType codecs
  implicit val dataTypeEncoder: Encoder[DataType] = Encoder.instance {
    // Common types
    case v: VarChar =>
      CirceJson.obj("type" -> CirceJson.fromString("VarChar"), "length" -> CirceJson.fromInt(v.length))
    case c: SqlChar =>
      CirceJson.obj("type" -> CirceJson.fromString("Char"), "length" -> CirceJson.fromInt(c.length))
    case SqlInteger => CirceJson.obj("type" -> CirceJson.fromString("Integer"))
    case SqlBigInt => CirceJson.obj("type" -> CirceJson.fromString("BigInt"))
    case SmallInt => CirceJson.obj("type" -> CirceJson.fromString("SmallInt"))
    case d: Decimal =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("Decimal"),
        "precision" -> CirceJson.fromInt(d.precision),
        "scale" -> CirceJson.fromInt(d.scale)
      )
    case n: Numeric =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("Numeric"),
        "precision" -> CirceJson.fromInt(n.precision),
        "scale" -> CirceJson.fromInt(n.scale)
      )
    case Real => CirceJson.obj("type" -> CirceJson.fromString("Real"))
    case DoublePrecision => CirceJson.obj("type" -> CirceJson.fromString("DoublePrecision"))
    case SqlDate => CirceJson.obj("type" -> CirceJson.fromString("Date"))
    case t: SqlTime =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("Time"),
        "precision" -> t.precision.asJson,
        "withTimeZone" -> CirceJson.fromBoolean(t.withTimeZone)
      )
    case t: SqlTimestamp =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("Timestamp"),
        "precision" -> t.precision.asJson,
        "withTimeZone" -> CirceJson.fromBoolean(t.withTimeZone)
      )
    case SqlBoolean => CirceJson.obj("type" -> CirceJson.fromString("Boolean"))
    case SqlText => CirceJson.obj("type" -> CirceJson.fromString("Text"))
    case SqlBlob => CirceJson.obj("type" -> CirceJson.fromString("Blob"))
    case SqlClob => CirceJson.obj("type" -> CirceJson.fromString("Clob"))
    // Oracle-specific types
    case v: Varchar2 =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("Varchar2"),
        "length" -> CirceJson.fromInt(v.length),
        "sizeSemantics" -> v.sizeSemantics.asJson
      )
    case n: NVarchar2 =>
      CirceJson.obj("type" -> CirceJson.fromString("NVarchar2"), "length" -> CirceJson.fromInt(n.length))
    case c: OracleChar =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("OracleChar"),
        "length" -> CirceJson.fromInt(c.length),
        "sizeSemantics" -> c.sizeSemantics.asJson
      )
    case n: NChar =>
      CirceJson.obj("type" -> CirceJson.fromString("NChar"), "length" -> CirceJson.fromInt(n.length))
    case n: Number =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("Number"),
        "precision" -> n.precision.asJson,
        "scale" -> n.scale.asJson
      )
    case BinaryFloat => CirceJson.obj("type" -> CirceJson.fromString("BinaryFloat"))
    case BinaryDouble => CirceJson.obj("type" -> CirceJson.fromString("BinaryDouble"))
    case r: Raw =>
      CirceJson.obj("type" -> CirceJson.fromString("Raw"), "length" -> CirceJson.fromInt(r.length))
    case OracleLong => CirceJson.obj("type" -> CirceJson.fromString("Long"))
    case LongRaw => CirceJson.obj("type" -> CirceJson.fromString("LongRaw"))
    case OracleRowid => CirceJson.obj("type" -> CirceJson.fromString("Rowid"))
    case u: URowid =>
      CirceJson.obj("type" -> CirceJson.fromString("URowid"), "length" -> u.length.asJson)
    case XMLType => CirceJson.obj("type" -> CirceJson.fromString("XMLType"))
    case i: IntervalYearToMonth =>
      CirceJson.obj("type" -> CirceJson.fromString("IntervalYearToMonth"), "precision" -> i.precision.asJson)
    case i: IntervalDayToSecond =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("IntervalDayToSecond"),
        "dayPrecision" -> i.dayPrecision.asJson,
        "secondPrecision" -> i.secondPrecision.asJson
      )
    // Postgres-specific types
    case PostgresSerial => CirceJson.obj("type" -> CirceJson.fromString("Serial"))
    case PostgresBigSerial => CirceJson.obj("type" -> CirceJson.fromString("BigSerial"))
    case PostgresSmallSerial => CirceJson.obj("type" -> CirceJson.fromString("SmallSerial"))
    case PostgresUuid => CirceJson.obj("type" -> CirceJson.fromString("Uuid"))
    case PostgresJson => CirceJson.obj("type" -> CirceJson.fromString("Json"))
    case PostgresJsonb => CirceJson.obj("type" -> CirceJson.fromString("Jsonb"))
    case PostgresInet => CirceJson.obj("type" -> CirceJson.fromString("Inet"))
    case PostgresCidr => CirceJson.obj("type" -> CirceJson.fromString("Cidr"))
    case PostgresMacAddr => CirceJson.obj("type" -> CirceJson.fromString("MacAddr"))
    case MacAddr8 => CirceJson.obj("type" -> CirceJson.fromString("MacAddr8"))
    case Money => CirceJson.obj("type" -> CirceJson.fromString("Money"))
    case Bytea => CirceJson.obj("type" -> CirceJson.fromString("Bytea"))
    case b: Bit => CirceJson.obj("type" -> CirceJson.fromString("Bit"), "length" -> b.length.asJson)
    case bv: BitVarying =>
      CirceJson.obj("type" -> CirceJson.fromString("BitVarying"), "length" -> bv.length.asJson)
    case PgPoint => CirceJson.obj("type" -> CirceJson.fromString("Point"))
    case PgLine => CirceJson.obj("type" -> CirceJson.fromString("Line"))
    case PgBox => CirceJson.obj("type" -> CirceJson.fromString("Box"))
    case PgCircle => CirceJson.obj("type" -> CirceJson.fromString("Circle"))
    case PgPolygon => CirceJson.obj("type" -> CirceJson.fromString("Polygon"))
    case PgPath => CirceJson.obj("type" -> CirceJson.fromString("Path"))
    case TsVector => CirceJson.obj("type" -> CirceJson.fromString("TsVector"))
    case TsQuery => CirceJson.obj("type" -> CirceJson.fromString("TsQuery"))
    case a: PgArray[?] =>
      CirceJson.obj("type" -> CirceJson.fromString("Array"), "elementType" -> dataTypeEncoder(a.elementType))
  }

  implicit val dataTypeDecoder: Decoder[DataType] = Decoder.instance { c =>
    c.get[String]("type").flatMap {
      // Common types
      case "VarChar" => c.get[Int]("length").map(VarChar.apply)
      case "Char" => c.get[Int]("length").map(SqlChar.apply)
      case "Integer" => Right(SqlInteger)
      case "BigInt" => Right(SqlBigInt)
      case "SmallInt" => Right(SmallInt)
      case "Decimal" =>
        for {
          p <- c.get[Int]("precision")
          s <- c.get[Int]("scale")
        } yield Decimal(p, s)
      case "Numeric" =>
        for {
          p <- c.get[Int]("precision")
          s <- c.get[Int]("scale")
        } yield Numeric(p, s)
      case "Real" => Right(Real)
      case "DoublePrecision" => Right(DoublePrecision)
      case "Date" => Right(SqlDate)
      case "Time" =>
        for {
          precision <- c.get[Option[Int]]("precision")
          withTimeZone <- c.get[Boolean]("withTimeZone")
        } yield SqlTime(precision, withTimeZone)
      case "Timestamp" =>
        for {
          precision <- c.get[Option[Int]]("precision")
          withTimeZone <- c.getOrElse[Boolean]("withTimeZone")(false)
        } yield SqlTimestamp(precision, withTimeZone)
      case "Boolean" => Right(SqlBoolean)
      case "Text" => Right(SqlText)
      case "Blob" => Right(SqlBlob)
      case "Clob" => Right(SqlClob)
      // Oracle-specific types
      case "Varchar2" =>
        for {
          l <- c.get[Int]("length")
          ss <- c.getOrElse[SizeSemantics]("sizeSemantics")(Bytes)
        } yield Varchar2(l, ss)
      case "NVarchar2" => c.get[Int]("length").map(NVarchar2.apply)
      case "OracleChar" =>
        for {
          l <- c.get[Int]("length")
          ss <- c.getOrElse[SizeSemantics]("sizeSemantics")(Bytes)
        } yield OracleChar(l, ss)
      case "NChar" => c.get[Int]("length").map(NChar.apply)
      case "Number" =>
        for {
          p <- c.get[Option[Int]]("precision")
          s <- c.get[Option[Int]]("scale")
        } yield Number(p, s)
      case "BinaryFloat" => Right(BinaryFloat)
      case "BinaryDouble" => Right(BinaryDouble)
      case "Raw" => c.get[Int]("length").map(Raw.apply)
      case "Long" => Right(OracleLong)
      case "LongRaw" => Right(LongRaw)
      case "Rowid" => Right(OracleRowid)
      case "URowid" => c.get[Option[Int]]("length").map(URowid.apply)
      case "XMLType" => Right(XMLType)
      case "IntervalYearToMonth" => c.get[Option[Int]]("precision").map(IntervalYearToMonth.apply)
      case "IntervalDayToSecond" =>
        for {
          dp <- c.get[Option[Int]]("dayPrecision")
          sp <- c.get[Option[Int]]("secondPrecision")
        } yield IntervalDayToSecond(dp, sp)
      // Postgres-specific types
      case "Serial" => Right(PostgresSerial)
      case "BigSerial" => Right(PostgresBigSerial)
      case "SmallSerial" => Right(PostgresSmallSerial)
      case "Uuid" => Right(PostgresUuid)
      case "Json" => Right(PostgresJson)
      case "Jsonb" => Right(PostgresJsonb)
      case "Inet" => Right(PostgresInet)
      case "Cidr" => Right(PostgresCidr)
      case "MacAddr" => Right(PostgresMacAddr)
      case "MacAddr8" => Right(MacAddr8)
      case "Money" => Right(Money)
      case "Bytea" => Right(Bytea)
      case "Bit" => c.get[Option[Int]]("length").map(Bit.apply)
      case "BitVarying" => c.get[Option[Int]]("length").map(BitVarying.apply)
      case "Point" => Right(PgPoint)
      case "Line" => Right(PgLine)
      case "Box" => Right(PgBox)
      case "Circle" => Right(PgCircle)
      case "Polygon" => Right(PgPolygon)
      case "Path" => Right(PgPath)
      case "TsVector" => Right(TsVector)
      case "TsQuery" => Right(TsQuery)
      case "Array" => c.get[DataType]("elementType")(dataTypeDecoder).map(PgArray.apply)
      case other => Left(DecodingFailure(s"Unknown DataType: $other", c.history))
    }
  }

  // Enum codecs
  implicit val arithmeticOpEncoder: Encoder[Arithmetic.Op] = Encoder.encodeString.contramap(_.toString)
  implicit val arithmeticOpDecoder: Decoder[Arithmetic.Op] = Decoder.decodeString.emap { s =>
    try Right(Arithmetic.withName(s).asInstanceOf[Arithmetic.Op])
    catch { case _: NoSuchElementException => Left(s"Unknown Arithmetic.Op: $s") }
  }

  implicit val comparisonOpEncoder: Encoder[Comparison.Op] = Encoder.encodeString.contramap(_.toString)
  implicit val comparisonOpDecoder: Decoder[Comparison.Op] = Decoder.decodeString.emap { s =>
    try Right(Comparison.withName(s).asInstanceOf[Comparison.Op])
    catch { case _: NoSuchElementException => Left(s"Unknown Comparison.Op: $s") }
  }

  implicit val joinTypeEncoder: Encoder[Join.Type] = Encoder.encodeString.contramap(_.toString)
  implicit val joinTypeDecoder: Decoder[Join.Type] = Decoder.decodeString.emap { s =>
    try Right(Join.withName(s).asInstanceOf[Join.Type])
    catch { case _: NoSuchElementException => Left(s"Unknown Join.Type: $s") }
  }

  implicit val unionTypeEncoder: Encoder[Union.Type] = Encoder.encodeString.contramap(_.toString)
  implicit val unionTypeDecoder: Decoder[Union.Type] = Decoder.decodeString.emap { s =>
    try Right(Union.withName(s).asInstanceOf[Union.Type])
    catch { case _: NoSuchElementException => Left(s"Unknown Union.Type: $s") }
  }

  // Expression codecs (recursive)
  implicit val expressionEncoder: Encoder[Expression] = Encoder.instance {
    case lp: LongPrimitive =>
      CirceJson.obj("type" -> CirceJson.fromString("LongPrimitive"), "value" -> CirceJson.fromLong(lp.v))
    case dp: DoublePrimitive =>
      CirceJson.obj("type" -> CirceJson.fromString("DoublePrimitive"), "value" -> CirceJson.fromDoubleOrNull(dp.v))
    case sp: StringPrimitive =>
      CirceJson.obj("type" -> CirceJson.fromString("StringPrimitive"), "value" -> CirceJson.fromString(sp.v))
    case bp: BooleanPrimitive =>
      CirceJson.obj("type" -> CirceJson.fromString("BooleanPrimitive"), "value" -> CirceJson.fromBoolean(bp.v))
    case NullPrimitive() => CirceJson.obj("type" -> CirceJson.fromString("NullPrimitive"))
    case c: Column =>
      CirceJson.obj("type" -> CirceJson.fromString("Column"), "column" -> c.column.asJson, "table" -> c.table.asJson)
    case a: Arithmetic =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("Arithmetic"),
        "lhs" -> a.lhs.asJson,
        "op" -> a.op.asJson,
        "rhs" -> a.rhs.asJson
      )
    case c: Comparison =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("Comparison"),
        "lhs" -> c.lhs.asJson,
        "op" -> c.op.asJson,
        "rhs" -> c.rhs.asJson
      )
    case f: Function =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("Function"),
        "name" -> f.name.asJson,
        "params" -> f.params.asJson,
        "distinct" -> CirceJson.fromBoolean(f.distinct)
      )
    case wf: WindowFunction =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("WindowFunction"),
        "function" -> wf.function.asJson,
        "partitionBy" -> wf.partitionBy.asJson,
        "orderBy" -> wf.orderBy.asJson
      )
    case JDBCVar() => CirceJson.obj("type" -> CirceJson.fromString("JDBCVar"))
    case sq: Subquery => CirceJson.obj("type" -> CirceJson.fromString("Subquery"), "query" -> sq.query.asJson)
    case cwe: CaseWhenElse =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("CaseWhenElse"),
        "target" -> cwe.target.asJson,
        "cases" -> cwe.cases.map { case (e1, e2) => CirceJson.obj("when" -> e1.asJson, "then" -> e2.asJson) }.asJson,
        "otherwise" -> cwe.otherwise.asJson
      )
    case in: IsNull => CirceJson.obj("type" -> CirceJson.fromString("IsNull"), "target" -> in.target.asJson)
    case n: Not => CirceJson.obj("type" -> CirceJson.fromString("Not"), "target" -> n.target.asJson)
    case cast: Cast =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("Cast"),
        "expression" -> cast.expression.asJson,
        "typeName" -> cast.t.asJson
      )
    case ie: InExpression =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("InExpression"),
        "expression" -> ie.expression.asJson,
        "source" -> (ie.source match {
          case Left(exprs) => CirceJson.obj("type" -> CirceJson.fromString("Expressions"), "values" -> exprs.asJson)
          case Right(query) => CirceJson.obj("type" -> CirceJson.fromString("SelectBody"), "query" -> query.asJson)
        })
      )
    case _: NegatableExpression => // Handled by IsNull and InExpression cases above
      throw new IllegalStateException("Unhandled NegatableExpression subtype")
  }

  implicit val expressionDecoder: Decoder[Expression] = Decoder.instance { c =>
    c.get[String]("type").flatMap {
      case "LongPrimitive" => c.get[Long]("value").map(LongPrimitive.apply)
      case "DoublePrimitive" => c.get[Double]("value").map(DoublePrimitive.apply)
      case "StringPrimitive" => c.get[String]("value").map(StringPrimitive.apply)
      case "BooleanPrimitive" => c.get[scala.Boolean]("value").map(BooleanPrimitive.apply)
      case "NullPrimitive" => Right(NullPrimitive())
      case "Column" =>
        for {
          col <- c.get[Name]("column")
          table <- c.get[Option[Name]]("table")
        } yield Column(col, table)
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
          distinct <- c.get[scala.Boolean]("distinct")
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
          cases <- c.get[Seq[CirceJson]]("cases").flatMap { jsons =>
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
      case "Not" => c.get[Expression]("target").map(Not.apply)
      case "Cast" =>
        for {
          expr <- c.get[Expression]("expression")
          typeName <- c.get[Name]("typeName")
        } yield Cast(expr, typeName)
      case "InExpression" =>
        for {
          expr <- c.get[Expression]("expression")
          source <- c.get[CirceJson]("source").flatMap { json =>
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

  // SelectTarget codecs (using circe-generic-extras)
  implicit val selectTargetEncoder: Encoder[SelectTarget] = deriveConfiguredEncoder[SelectTarget]
  implicit val selectTargetDecoder: Decoder[SelectTarget] = deriveConfiguredDecoder[SelectTarget]

  // OrderBy codec
  implicit val orderByEncoder: Encoder[OrderBy] = deriveEncoder[OrderBy]
  implicit val orderByDecoder: Decoder[OrderBy] = deriveDecoder[OrderBy]

  // FromElement codecs (using circe-generic-extras)
  implicit val fromElementEncoder: Encoder[FromElement] = deriveConfiguredEncoder[FromElement]
  implicit val fromElementDecoder: Decoder[FromElement] = deriveConfiguredDecoder[FromElement]

  // SelectBody codec (recursive)
  implicit val selectBodyEncoder: Encoder[SelectBody] = deriveEncoder[SelectBody]
  implicit val selectBodyDecoder: Decoder[SelectBody] = deriveDecoder[SelectBody]

  // ColumnAnnotation codecs (using circe-generic-extras)
  implicit val columnAnnotationEncoder: Encoder[ColumnAnnotation] = deriveConfiguredEncoder[ColumnAnnotation]
  implicit val columnAnnotationDecoder: Decoder[ColumnAnnotation] = deriveConfiguredDecoder[ColumnAnnotation]

  // PrimitiveValue codec (needed for ColumnDefinition)
  implicit val primitiveValueEncoder: Encoder[PrimitiveValue] = expressionEncoder.contramap(identity[Expression])
  implicit val primitiveValueDecoder: Decoder[PrimitiveValue] = expressionDecoder.emap {
    case pv: PrimitiveValue => Right(pv)
    case other => Left(s"Expected PrimitiveValue but got ${other.getClass.getSimpleName}")
  }

  // ColumnDefinition codec
  implicit val columnDefinitionEncoder: Encoder[ColumnDefinition] = deriveEncoder[ColumnDefinition]
  implicit val columnDefinitionDecoder: Decoder[ColumnDefinition] = deriveDecoder[ColumnDefinition]

  // ReferentialAction codecs (schema types, needed before TableAnnotation)
  implicit val referentialActionEncoder: Encoder[ReferentialAction] = Encoder.encodeString.contramap {
    case NoAction => "NO ACTION"
    case Restrict => "RESTRICT"
    case Cascade => "CASCADE"
    case SetNull => "SET NULL"
    case SetDefault => "SET DEFAULT"
    case other => other.toSql
  }
  implicit val referentialActionDecoder: Decoder[ReferentialAction] = Decoder.decodeString.emap {
    case "NO ACTION" => Right(NoAction)
    case "RESTRICT" => Right(Restrict)
    case "CASCADE" => Right(Cascade)
    case "SET NULL" => Right(SetNull)
    case "SET DEFAULT" => Right(SetDefault)
    case other => Left(s"Unknown ReferentialAction: $other")
  }

  // TableAnnotation codecs (using circe-generic-extras)
  implicit val tableAnnotationEncoder: Encoder[TableAnnotation] = deriveConfiguredEncoder[TableAnnotation]
  implicit val tableAnnotationDecoder: Decoder[TableAnnotation] = deriveConfiguredDecoder[TableAnnotation]

  // InsertValues codecs (using circe-generic-extras)
  implicit val insertValuesEncoder: Encoder[InsertValues] = deriveConfiguredEncoder[InsertValues]
  implicit val insertValuesDecoder: Decoder[InsertValues] = deriveConfiguredDecoder[InsertValues]

  // WithClause codec
  implicit val withClauseEncoder: Encoder[WithClause] = deriveEncoder[WithClause]
  implicit val withClauseDecoder: Decoder[WithClause] = deriveDecoder[WithClause]

  // AlterViewAction codecs (using circe-generic-extras)
  implicit val alterViewActionEncoder: Encoder[AlterViewAction] = deriveConfiguredEncoder[AlterViewAction]
  implicit val alterViewActionDecoder: Decoder[AlterViewAction] = deriveConfiguredDecoder[AlterViewAction]

  // Statement codecs (base and common)
  implicit val statementEncoder: Encoder[Statement] = Encoder.instance {
    case EmptyStatement() => CirceJson.obj("type" -> CirceJson.fromString("EmptyStatement"))
    case u: Unparseable =>
      CirceJson.obj("type" -> CirceJson.fromString("Unparseable"), "content" -> CirceJson.fromString(u.content))
    case s: Select =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("Select"),
        "body" -> s.body.asJson,
        "withClause" -> s.withClause.asJson
      )
    case u: Update =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("Update"),
        "table" -> u.table.asJson,
        "set" -> u.set.map { case (n, e) => CirceJson.obj("name" -> n.asJson, "expression" -> e.asJson) }.asJson,
        "where" -> u.where.asJson
      )
    case d: Delete =>
      CirceJson.obj("type" -> CirceJson.fromString("Delete"), "table" -> d.table.asJson, "where" -> d.where.asJson)
    case i: Insert =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("Insert"),
        "table" -> i.table.asJson,
        "columns" -> i.columns.asJson,
        "values" -> i.values.asJson,
        "orReplace" -> CirceJson.fromBoolean(i.orReplace)
      )
    case ct: CreateTable =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("CreateTable"),
        "name" -> ct.name.asJson,
        "orReplace" -> CirceJson.fromBoolean(ct.orReplace),
        "columns" -> ct.columns.asJson,
        "annotations" -> ct.annotations.asJson
      )
    case cta: CreateTableAs =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("CreateTableAs"),
        "name" -> cta.name.asJson,
        "orReplace" -> CirceJson.fromBoolean(cta.orReplace),
        "query" -> cta.query.asJson
      )
    case cv: CreateView =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("CreateView"),
        "name" -> cv.name.asJson,
        "orReplace" -> CirceJson.fromBoolean(cv.orReplace),
        "query" -> cv.query.asJson,
        "materialized" -> CirceJson.fromBoolean(cv.materialized),
        "temporary" -> CirceJson.fromBoolean(cv.temporary)
      )
    case av: AlterView =>
      CirceJson.obj("type" -> CirceJson.fromString("AlterView"), "name" -> av.name.asJson, "action" -> av.action.asJson)
    case dt: DropTable =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("DropTable"),
        "name" -> dt.name.asJson,
        "ifExists" -> CirceJson.fromBoolean(dt.ifExists)
      )
    case dv: DropView =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("DropView"),
        "name" -> dv.name.asJson,
        "ifExists" -> CirceJson.fromBoolean(dv.ifExists)
      )
    case e: Explain => CirceJson.obj("type" -> CirceJson.fromString("Explain"), "query" -> e.query.asJson)
    // Oracle statements
    case os: OracleStatement => oracleStatementEncoder(os)
  }

  implicit val statementDecoder: Decoder[Statement] = Decoder.instance { c =>
    c.get[String]("type").flatMap {
      case "EmptyStatement" => Right(EmptyStatement())
      case "Unparseable" => c.get[String]("content").map(Unparseable.apply)
      case "Select" =>
        for {
          body <- c.get[SelectBody]("body")
          withClause <- c.get[Seq[WithClause]]("withClause")
        } yield Select(body, withClause)
      case "Update" =>
        for {
          table <- c.get[Name]("table")
          set <- c.get[Seq[CirceJson]]("set").flatMap { jsons =>
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
          orReplace <- c.get[Boolean]("orReplace")
        } yield Insert(table, columns, values, orReplace)
      case "CreateTable" =>
        for {
          name <- c.get[Name]("name")
          orReplace <- c.get[Boolean]("orReplace")
          columns <- c.get[Seq[ColumnDefinition]]("columns")
          annotations <- c.get[Seq[TableAnnotation]]("annotations")
        } yield CreateTable(name, orReplace, columns, annotations)
      case "CreateTableAs" =>
        for {
          name <- c.get[Name]("name")
          orReplace <- c.get[Boolean]("orReplace")
          query <- c.get[SelectBody]("query")
        } yield CreateTableAs(name, orReplace, query)
      case "CreateView" =>
        for {
          name <- c.get[Name]("name")
          orReplace <- c.get[Boolean]("orReplace")
          query <- c.get[SelectBody]("query")
          materialized <- c.get[Boolean]("materialized")
          temporary <- c.get[Boolean]("temporary")
        } yield CreateView(name, orReplace, query, materialized, temporary)
      case "AlterView" =>
        for {
          name <- c.get[Name]("name")
          action <- c.get[AlterViewAction]("action")
        } yield AlterView(name, action)
      case "DropTable" =>
        for {
          name <- c.get[Name]("name")
          ifExists <- c.get[Boolean]("ifExists")
        } yield DropTable(name, ifExists)
      case "DropView" =>
        for {
          name <- c.get[Name]("name")
          ifExists <- c.get[Boolean]("ifExists")
        } yield DropView(name, ifExists)
      case "Explain" => c.get[SelectBody]("query").map(Explain.apply)
      case other => oracleStatementDecoderHelper(c, other)
    }
  }

  // Schema constraint type codecs
  implicit val primaryKeyEncoder: Encoder[PrimaryKey] = deriveEncoder[PrimaryKey]
  implicit val primaryKeyDecoder: Decoder[PrimaryKey] = deriveDecoder[PrimaryKey]
  implicit val foreignKeyEncoder: Encoder[ForeignKey] = deriveEncoder[ForeignKey]
  implicit val foreignKeyDecoder: Decoder[ForeignKey] = deriveDecoder[ForeignKey]
  implicit val uniqueEncoder: Encoder[Unique] = deriveEncoder[Unique]
  implicit val uniqueDecoder: Decoder[Unique] = deriveDecoder[Unique]
  implicit val checkEncoder: Encoder[Check] = deriveEncoder[Check]
  implicit val checkDecoder: Decoder[Check] = deriveDecoder[Check]

  // TableConstraint codecs (now using schema types with wrapped constraints)
  implicit val tableConstraintEncoder: Encoder[TableConstraint] = deriveConfiguredEncoder[TableConstraint]
  implicit val tableConstraintDecoder: Decoder[TableConstraint] = deriveConfiguredDecoder[TableConstraint]

  implicit val columnModificationEncoder: Encoder[ColumnModification] = deriveEncoder[ColumnModification]
  implicit val columnModificationDecoder: Decoder[ColumnModification] = deriveDecoder[ColumnModification]

  implicit val storageClauseEncoder: Encoder[StorageClause] = deriveEncoder[StorageClause]
  implicit val storageClauseDecoder: Decoder[StorageClause] = deriveDecoder[StorageClause]

  // Custom codec for (Name, Expression) tuples to encode as objects with "name" and "expression" fields
  implicit val nameExpressionTupleEncoder: Encoder[(Name, Expression)] = Encoder.instance { case (name, expr) =>
    CirceJson.obj("name" -> name.asJson, "expression" -> expr.asJson)
  }
  implicit val nameExpressionTupleDecoder: Decoder[(Name, Expression)] = Decoder.instance { c =>
    for {
      name <- c.get[Name]("name")
      expr <- c.get[Expression]("expression")
    } yield (name, expr)
  }

  // Custom codec for Either[SelectBody, Seq[ColumnDefinition]] used in OracleCreateTable.schema
  // Encodes as an object with "type" field ("SelectBody" or "Columns") and corresponding data
  implicit val oracleCreateTableSchemaEncoder: Encoder[Either[SelectBody, Seq[ColumnDefinition]]] =
    Encoder.instance {
      case Left(query) =>
        CirceJson.obj("type" -> CirceJson.fromString("SelectBody"), "query" -> query.asJson)
      case Right(columns) =>
        CirceJson.obj("type" -> CirceJson.fromString("Columns"), "columns" -> columns.asJson)
    }
  implicit val oracleCreateTableSchemaDecoder: Decoder[Either[SelectBody, Seq[ColumnDefinition]]] =
    Decoder.instance { c =>
      c.get[String]("type").flatMap {
        case "SelectBody" => c.get[SelectBody]("query").map(Left.apply)
        case "Columns" => c.get[Seq[ColumnDefinition]]("columns").map(Right.apply)
        case other =>
          Left(DecodingFailure(s"Unknown OracleCreateTable schema type: $other", c.history))
      }
    }

  // ParserAlterTableAction codecs (needed for OracleAlterTable)
  // Note: ParserAlterTableAction = AlterTableAction[DataType, NoColumnOptions.type]
  implicit val parserAlterTableActionEncoder: Encoder[ParserAlterTableAction] = Encoder.instance {
    case AddColumn(col) =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("AddColumn"),
        "name" -> col.name.asJson,
        "dataType" -> dataTypeEncoder(col.dataType),
        "nullable" -> CirceJson.fromBoolean(col.nullable),
        "default" -> col.default.asJson
      )
    case DropColumn(name, cascade) =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("DropColumn"),
        "name" -> CirceJson.fromString(name),
        "cascade" -> CirceJson.fromBoolean(cascade)
      )
    case ModifyColumn(col) =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("ModifyColumn"),
        "name" -> col.name.asJson,
        "dataType" -> dataTypeEncoder(col.dataType),
        "nullable" -> CirceJson.fromBoolean(col.nullable),
        "default" -> col.default.asJson
      )
    case RenameColumn(oldName, newName) =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("RenameColumn"),
        "oldName" -> CirceJson.fromString(oldName),
        "newName" -> CirceJson.fromString(newName)
      )
    case AddConstraint(constraint) =>
      CirceJson.obj("type" -> CirceJson.fromString("AddConstraint"), "constraint" -> tableConstraintEncoder(constraint))
    case DropConstraint(name, cascade) =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("DropConstraint"),
        "name" -> CirceJson.fromString(name),
        "cascade" -> CirceJson.fromBoolean(cascade)
      )
    case atm: AlterTableModify =>
      CirceJson.obj("type" -> CirceJson.fromString("AlterTableModify"), "modifications" -> atm.modifications.asJson)
    case ata: AlterTableAdd =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("AlterTableAdd"),
        "columns" -> ata.columns.asJson,
        "annotations" -> ata.annotations.asJson
      )
  }

  implicit val parserAlterTableActionDecoder: Decoder[ParserAlterTableAction] = Decoder.instance { c =>
    c.get[String]("type").flatMap {
      case "AddColumn" =>
        for {
          name <- c.get[String]("name")
          dataType <- c.get[DataType]("dataType")
          nullable <- c.get[Boolean]("nullable")
          default <- c.get[Option[String]]("default")
        } yield AddColumn(
          SchemaColumn[DataType, NoColumnOptions.type](name, dataType, nullable, default, NoColumnOptions)
        )
      case "DropColumn" =>
        for {
          name <- c.get[String]("name")
          cascade <- c.get[Boolean]("cascade")
        } yield DropColumn[DataType, NoColumnOptions.type](name, cascade)
      case "ModifyColumn" =>
        for {
          name <- c.get[String]("name")
          dataType <- c.get[DataType]("dataType")
          nullable <- c.get[Boolean]("nullable")
          default <- c.get[Option[String]]("default")
        } yield ModifyColumn(
          SchemaColumn[DataType, NoColumnOptions.type](name, dataType, nullable, default, NoColumnOptions)
        )
      case "RenameColumn" =>
        for {
          oldName <- c.get[String]("oldName")
          newName <- c.get[String]("newName")
        } yield RenameColumn[DataType, NoColumnOptions.type](oldName, newName)
      case "AddConstraint" =>
        c.get[TableConstraint]("constraint").map(AddConstraint[DataType, NoColumnOptions.type](_))
      case "DropConstraint" =>
        for {
          name <- c.get[String]("name")
          cascade <- c.get[Boolean]("cascade")
        } yield DropConstraint[DataType, NoColumnOptions.type](name, cascade)
      case "AlterTableModify" =>
        c.get[Seq[ColumnModification]]("modifications").map(AlterTableModify(_))
      case "AlterTableAdd" =>
        for {
          columns <- c.get[Seq[ColumnDefinition]]("columns")
          annotations <- c.get[Seq[TableAnnotation]]("annotations")
        } yield AlterTableAdd(columns, annotations)
      case other =>
        Left(DecodingFailure(s"Unknown ParserAlterTableAction type: $other", c.history))
    }
  }

  // OracleStatement codecs (using circe-generic-extras)
  // Note: The tuple codec above handles Update.set and OracleUpdate.set fields
  implicit val oracleStatementEncoder: Encoder[OracleStatement] = deriveConfiguredEncoder[OracleStatement]
  implicit val oracleStatementDecoder: Decoder[OracleStatement] = deriveConfiguredDecoder[OracleStatement]

  // Helper function to decode OracleStatement from Statement decoder
  def oracleStatementDecoderHelper(c: HCursor, typeName: String): Decoder.Result[Statement] = {
    // Use the automatic decoder for OracleStatement
    oracleStatementDecoder(c).left.map { failure =>
      DecodingFailure(s"Unknown Statement type: $typeName - ${failure.message}", c.history)
    }
  }
}
