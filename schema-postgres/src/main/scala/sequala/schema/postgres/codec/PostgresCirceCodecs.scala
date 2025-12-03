package sequala.schema.postgres.codec

import io.circe.{Decoder, DecodingFailure, Encoder}
import io.circe.Json as CirceJson
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.syntax.*
import sequala.schema.*
import sequala.schema.codec.{BaseCirceCodecs, CommonDataTypeCodecs}
import sequala.schema.codec.CommonDataTypeCodecs.given
import sequala.schema.postgres.*
import sequala.schema.postgres.Json as PgJson

object PostgresCirceCodecs extends BaseCirceCodecs, PostgresDialect:

  // Postgres-specific DataType encoder
  private given postgresSpecificEncoder: Encoder[PostgresSpecificDataType] = Encoder.instance {
    case Serial => CirceJson.obj("type" -> CirceJson.fromString("Serial"))
    case BigSerial => CirceJson.obj("type" -> CirceJson.fromString("BigSerial"))
    case SmallSerial => CirceJson.obj("type" -> CirceJson.fromString("SmallSerial"))
    case Uuid => CirceJson.obj("type" -> CirceJson.fromString("Uuid"))
    case PgJson => CirceJson.obj("type" -> CirceJson.fromString("Json"))
    case Jsonb => CirceJson.obj("type" -> CirceJson.fromString("Jsonb"))
    case Inet => CirceJson.obj("type" -> CirceJson.fromString("Inet"))
    case Cidr => CirceJson.obj("type" -> CirceJson.fromString("Cidr"))
    case MacAddr => CirceJson.obj("type" -> CirceJson.fromString("MacAddr"))
    case MacAddr8 => CirceJson.obj("type" -> CirceJson.fromString("MacAddr8"))
    case Money => CirceJson.obj("type" -> CirceJson.fromString("Money"))
    case Bytea => CirceJson.obj("type" -> CirceJson.fromString("Bytea"))
    case b: Bit => CirceJson.obj("type" -> CirceJson.fromString("Bit"), "length" -> b.length.asJson)
    case bv: BitVarying => CirceJson.obj("type" -> CirceJson.fromString("BitVarying"), "length" -> bv.length.asJson)
    case PgPoint => CirceJson.obj("type" -> CirceJson.fromString("Point"))
    case PgLine => CirceJson.obj("type" -> CirceJson.fromString("Line"))
    case PgBox => CirceJson.obj("type" -> CirceJson.fromString("Box"))
    case PgCircle => CirceJson.obj("type" -> CirceJson.fromString("Circle"))
    case PgPolygon => CirceJson.obj("type" -> CirceJson.fromString("Polygon"))
    case PgPath => CirceJson.obj("type" -> CirceJson.fromString("Path"))
    case TsVector => CirceJson.obj("type" -> CirceJson.fromString("TsVector"))
    case TsQuery => CirceJson.obj("type" -> CirceJson.fromString("TsQuery"))
    case a: PgArray[?] =>
      CirceJson.obj("type" -> CirceJson.fromString("Array"), "elementType" -> encodeDataType(a.elementType))
  }

  private def encodeDataType(dt: sequala.schema.DataType): CirceJson = dt match
    case cdt: CommonDataType => summon[Encoder[CommonDataType]].apply(cdt)
    case pdt: PostgresSpecificDataType => postgresSpecificEncoder(pdt)

  private def decodePostgresSpecific(c: io.circe.HCursor): Decoder.Result[PostgresSpecificDataType] =
    c.get[String]("type").flatMap {
      case "Serial" => Right(Serial)
      case "BigSerial" => Right(BigSerial)
      case "SmallSerial" => Right(SmallSerial)
      case "Uuid" => Right(Uuid)
      case "Json" => Right(PgJson)
      case "Jsonb" => Right(Jsonb)
      case "Inet" => Right(Inet)
      case "Cidr" => Right(Cidr)
      case "MacAddr" => Right(MacAddr)
      case "MacAddr8" => Right(MacAddr8)
      case "Money" => Right(Money)
      case "Bytea" => Right(Bytea)
      case "Bit" => c.get[Option[Int]]("length").map(l => Bit(l))
      case "BitVarying" => c.get[Option[Int]]("length").map(l => BitVarying(l))
      case "Point" => Right(PgPoint)
      case "Line" => Right(PgLine)
      case "Box" => Right(PgBox)
      case "Circle" => Right(PgCircle)
      case "Polygon" => Right(PgPolygon)
      case "Path" => Right(PgPath)
      case "TsVector" => Right(TsVector)
      case "TsQuery" => Right(TsQuery)
      case "Array" => c.get[PostgresDataType]("elementType")(using dataTypeDecoder).map(et => PgArray(et))
      case other => Left(DecodingFailure(s"Unknown PostgresSpecificDataType: $other", c.history))
    }

  // PostgresDataType = CommonDataType | PostgresSpecificDataType
  given dataTypeEncoder: Encoder[DataType] = Encoder.instance {
    case cdt: CommonDataType => summon[Encoder[CommonDataType]].apply(cdt)
    case pdt: PostgresSpecificDataType => postgresSpecificEncoder(pdt)
  }

  given dataTypeDecoder: Decoder[DataType] = Decoder.instance { c =>
    decodePostgresSpecific(c).orElse(CommonDataTypeCodecs.tryDecodeCommon(c))
  }

  // PostgresGeneratedColumn codec
  given Encoder[PostgresGeneratedColumn] = deriveEncoder[PostgresGeneratedColumn]
  given Decoder[PostgresGeneratedColumn] = deriveDecoder[PostgresGeneratedColumn]

  // PostgresColumnOptions codec
  given columnOptionsEncoder: Encoder[ColumnOptions] = deriveEncoder[PostgresColumnOptions]
  given columnOptionsDecoder: Decoder[ColumnOptions] = deriveDecoder[PostgresColumnOptions]

  // PostgresPartitionStrategy codec
  given Encoder[PostgresPartitionStrategy] = deriveConfiguredEncoder[PostgresPartitionStrategy]
  given Decoder[PostgresPartitionStrategy] = deriveConfiguredDecoder[PostgresPartitionStrategy]

  // PostgresPartitionSpec codec
  given Encoder[PostgresPartitionSpec] = deriveEncoder[PostgresPartitionSpec]
  given Decoder[PostgresPartitionSpec] = deriveDecoder[PostgresPartitionSpec]

  // PostgresTableOptions codec
  given tableOptionsEncoder: Encoder[TableOptions] = deriveEncoder[PostgresTableOptions]
  given tableOptionsDecoder: Decoder[TableOptions] = deriveDecoder[PostgresTableOptions]

  // Use CommonDropOptions encoder/decoder
  given dropOptionsEncoder: Encoder[DropOptions] = deriveEncoder[CommonDropOptions]
  given dropOptionsDecoder: Decoder[DropOptions] = deriveDecoder[CommonDropOptions]

  // Use CommonCreateViewOptions encoder/decoder
  given createViewOptionsEncoder: Encoder[CreateViewOptions] = deriveEncoder[CommonCreateViewOptions]
  given createViewOptionsDecoder: Decoder[CreateViewOptions] = deriveDecoder[CommonCreateViewOptions]

  // Use CommonDropViewOptions encoder/decoder
  given dropViewOptionsEncoder: Encoder[DropViewOptions] = deriveEncoder[CommonDropViewOptions]
  given dropViewOptionsDecoder: Decoder[DropViewOptions] = deriveDecoder[CommonDropViewOptions]

  // Use CommonIndexOptions encoder/decoder
  given indexOptionsEncoder: Encoder[IndexOptions] = deriveEncoder[CommonIndexOptions]
  given indexOptionsDecoder: Decoder[IndexOptions] = deriveDecoder[CommonIndexOptions]

  // Use CommonExplainOptions encoder/decoder
  given explainOptionsEncoder: Encoder[ExplainOptions] = deriveEncoder[CommonExplainOptions]
  given explainOptionsDecoder: Decoder[ExplainOptions] = deriveDecoder[CommonExplainOptions]

  // Standard AlterTableAction encoder/decoder for Postgres (no special Postgres-specific actions)
  given postgresAlterTableActionEncoder: Encoder[AlterTableAction[DataType, ColumnOptions]] = Encoder.instance {
    case ac: AddColumn[DataType, ColumnOptions] => addColumnEncoder(ac)
    case dc: DropColumn[DataType, ColumnOptions] => dropColumnEncoder(dc)
    case mc: ModifyColumn[DataType, ColumnOptions] => modifyColumnEncoder(mc)
    case rc: RenameColumn[DataType, ColumnOptions] => renameColumnEncoder(rc)
    case ac: AddConstraint[DataType, ColumnOptions] => addConstraintEncoder(ac)
    case dc: DropConstraint[DataType, ColumnOptions] => dropConstraintEncoder(dc)
  }

  given postgresAlterTableActionDecoder: Decoder[AlterTableAction[DataType, ColumnOptions]] = Decoder.instance { c =>
    c.get[String]("type").flatMap {
      case "AddColumn" => addColumnDecoder(c)
      case "DropColumn" => dropColumnDecoder(c)
      case "ModifyColumn" => modifyColumnDecoder(c)
      case "RenameColumn" => renameColumnDecoder(c)
      case "AddConstraint" => addConstraintDecoder(c)
      case "DropConstraint" => dropConstraintDecoder(c)
      case other => Left(DecodingFailure(s"Unknown AlterTableAction type: $other", c.history))
    }
  }

  // AlterTable encoder/decoder for Postgres
  given alterTableEncoder
    : Encoder[AlterTable[DataType, ColumnOptions, TableOptions, AlterTableAction[DataType, ColumnOptions]]] =
    Encoder.instance { at =>
      CirceJson.obj(
        "type" -> CirceJson.fromString("AlterTable"),
        "tableName" -> CirceJson.fromString(at.tableName),
        "actions" -> at.actions.asJson(using Encoder.encodeSeq(using postgresAlterTableActionEncoder))
      )
    }

  given alterTableDecoder
    : Decoder[AlterTable[DataType, ColumnOptions, TableOptions, AlterTableAction[DataType, ColumnOptions]]] =
    Decoder.instance { c =>
      for {
        tableName <- c.get[String]("tableName")
        actions <- c.get[Seq[AlterTableAction[DataType, ColumnOptions]]]("actions")(using
          Decoder.decodeSeq(using postgresAlterTableActionDecoder)
        )
      } yield AlterTable(tableName, actions)
    }
