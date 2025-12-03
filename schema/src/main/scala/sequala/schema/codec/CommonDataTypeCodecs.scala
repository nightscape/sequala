package sequala.schema.codec

import io.circe.{Decoder, DecodingFailure, Encoder, Json}
import io.circe.syntax.*
import sequala.schema.*

object CommonDataTypeCodecs:
  given Encoder[CommonDataType] = Encoder.instance {
    case v: VarChar =>
      Json.obj("type" -> Json.fromString("VarChar"), "length" -> Json.fromInt(v.length))
    case c: SqlChar =>
      Json.obj("type" -> Json.fromString("Char"), "length" -> Json.fromInt(c.length))
    case SqlInteger => Json.obj("type" -> Json.fromString("Integer"))
    case SqlBigInt => Json.obj("type" -> Json.fromString("BigInt"))
    case SmallInt => Json.obj("type" -> Json.fromString("SmallInt"))
    case d: Decimal =>
      Json.obj(
        "type" -> Json.fromString("Decimal"),
        "precision" -> Json.fromInt(d.precision),
        "scale" -> Json.fromInt(d.scale)
      )
    case n: Numeric =>
      Json.obj(
        "type" -> Json.fromString("Numeric"),
        "precision" -> Json.fromInt(n.precision),
        "scale" -> Json.fromInt(n.scale)
      )
    case Real => Json.obj("type" -> Json.fromString("Real"))
    case DoublePrecision => Json.obj("type" -> Json.fromString("DoublePrecision"))
    case SqlDate => Json.obj("type" -> Json.fromString("Date"))
    case t: SqlTime =>
      Json.obj(
        "type" -> Json.fromString("Time"),
        "precision" -> t.precision.asJson,
        "withTimeZone" -> Json.fromBoolean(t.withTimeZone)
      )
    case t: SqlTimestamp =>
      Json.obj(
        "type" -> Json.fromString("Timestamp"),
        "precision" -> t.precision.asJson,
        "withTimeZone" -> Json.fromBoolean(t.withTimeZone)
      )
    case SqlBoolean => Json.obj("type" -> Json.fromString("Boolean"))
    case SqlText => Json.obj("type" -> Json.fromString("Text"))
    case SqlBlob => Json.obj("type" -> Json.fromString("Blob"))
    case SqlClob => Json.obj("type" -> Json.fromString("Clob"))
  }

  given Decoder[CommonDataType] = Decoder.instance { c =>
    c.get[String]("type").flatMap {
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
      case other => Left(DecodingFailure(s"Unknown CommonDataType: $other", c.history))
    }
  }

  def tryDecodeCommon(c: io.circe.HCursor): Decoder.Result[CommonDataType] =
    summon[Decoder[CommonDataType]].tryDecode(c)
