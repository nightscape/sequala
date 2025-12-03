package sequala.schema.codec

import io.circe.{Decoder, Encoder}
import sequala.schema.*
import sequala.schema.codec.CommonDataTypeCodecs.given

object AnsiCirceCodecs extends BaseCirceCodecs, AnsiDialect:

  import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

  // For AnsiDialect, DataType = CommonDataType, so we can directly assign
  given dataTypeEncoder: Encoder[DataType] = CommonDataTypeCodecs.given_Encoder_CommonDataType
  given dataTypeDecoder: Decoder[DataType] = CommonDataTypeCodecs.given_Decoder_CommonDataType

  given columnOptionsEncoder: Encoder[ColumnOptions] = Encoder.instance(_ => io.circe.Json.obj())
  given columnOptionsDecoder: Decoder[ColumnOptions] = Decoder.const(NoColumnOptions)

  given tableOptionsEncoder: Encoder[TableOptions] = Encoder.instance(_ => io.circe.Json.obj())
  given tableOptionsDecoder: Decoder[TableOptions] = Decoder.const(NoTableOptions)

  given dropOptionsEncoder: Encoder[DropOptions] = deriveEncoder[CommonDropOptions]
  given dropOptionsDecoder: Decoder[DropOptions] = deriveDecoder[CommonDropOptions]

  given createViewOptionsEncoder: Encoder[CreateViewOptions] = deriveEncoder[CommonCreateViewOptions]
  given createViewOptionsDecoder: Decoder[CreateViewOptions] = deriveDecoder[CommonCreateViewOptions]

  given dropViewOptionsEncoder: Encoder[DropViewOptions] = deriveEncoder[CommonDropViewOptions]
  given dropViewOptionsDecoder: Decoder[DropViewOptions] = deriveDecoder[CommonDropViewOptions]

  given indexOptionsEncoder: Encoder[IndexOptions] = deriveEncoder[CommonIndexOptions]
  given indexOptionsDecoder: Decoder[IndexOptions] = deriveDecoder[CommonIndexOptions]

  given explainOptionsEncoder: Encoder[ExplainOptions] = deriveEncoder[CommonExplainOptions]
  given explainOptionsDecoder: Decoder[ExplainOptions] = deriveDecoder[CommonExplainOptions]
