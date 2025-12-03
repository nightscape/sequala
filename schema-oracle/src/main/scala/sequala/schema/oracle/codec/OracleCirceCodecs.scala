package sequala.schema.oracle.codec

import io.circe.{Decoder, DecodingFailure, Encoder, Json}
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.syntax.*
import sequala.schema.*
import sequala.schema.codec.{BaseCirceCodecs, CommonDataTypeCodecs}
import sequala.schema.codec.CommonDataTypeCodecs.given
import sequala.schema.oracle.*
import sequala.schema.statement.*

object OracleCirceCodecs extends BaseCirceCodecs, OracleDialect:

  // SizeSemantics codec
  given Encoder[SizeSemantics] = Encoder.encodeString.contramap {
    case Bytes => "BYTE"
    case Chars => "CHAR"
  }
  given Decoder[SizeSemantics] = Decoder.decodeString.emap {
    case "BYTE" => Right(Bytes)
    case "CHAR" => Right(Chars)
    case other => Left(s"Unknown SizeSemantics: $other")
  }

  // Oracle-specific DataType encoder
  private given oracleSpecificEncoder: Encoder[OracleSpecificDataType] = Encoder.instance {
    case v: Varchar2 =>
      Json.obj(
        "type" -> Json.fromString("Varchar2"),
        "length" -> Json.fromInt(v.length),
        "sizeSemantics" -> v.sizeSemantics.asJson
      )
    case n: NVarchar2 =>
      Json.obj("type" -> Json.fromString("NVarchar2"), "length" -> Json.fromInt(n.length))
    case c: OracleChar =>
      Json.obj(
        "type" -> Json.fromString("OracleChar"),
        "length" -> Json.fromInt(c.length),
        "sizeSemantics" -> c.sizeSemantics.asJson
      )
    case n: NChar =>
      Json.obj("type" -> Json.fromString("NChar"), "length" -> Json.fromInt(n.length))
    case n: Number =>
      Json.obj("type" -> Json.fromString("Number"), "precision" -> n.precision.asJson, "scale" -> n.scale.asJson)
    case BinaryFloat => Json.obj("type" -> Json.fromString("BinaryFloat"))
    case BinaryDouble => Json.obj("type" -> Json.fromString("BinaryDouble"))
    case r: Raw =>
      Json.obj("type" -> Json.fromString("Raw"), "length" -> Json.fromInt(r.length))
    case OracleLong => Json.obj("type" -> Json.fromString("Long"))
    case LongRaw => Json.obj("type" -> Json.fromString("LongRaw"))
    case OracleRowid => Json.obj("type" -> Json.fromString("Rowid"))
    case u: URowid =>
      Json.obj("type" -> Json.fromString("URowid"), "length" -> u.length.asJson)
    case XMLType => Json.obj("type" -> Json.fromString("XMLType"))
    case i: IntervalYearToMonth =>
      Json.obj("type" -> Json.fromString("IntervalYearToMonth"), "precision" -> i.precision.asJson)
    case i: IntervalDayToSecond =>
      Json.obj(
        "type" -> Json.fromString("IntervalDayToSecond"),
        "dayPrecision" -> i.dayPrecision.asJson,
        "secondPrecision" -> i.secondPrecision.asJson
      )
  }

  private def decodeOracleSpecific(c: io.circe.HCursor): Decoder.Result[OracleSpecificDataType] =
    c.get[String]("type").flatMap {
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
      case other => Left(DecodingFailure(s"Unknown OracleSpecificDataType: $other", c.history))
    }

  // OracleDataType = CommonDataType | OracleSpecificDataType
  given dataTypeEncoder: Encoder[DataType] = Encoder.instance {
    case cdt: CommonDataType => summon[Encoder[CommonDataType]].apply(cdt)
    case odt: OracleSpecificDataType => oracleSpecificEncoder(odt)
  }

  given dataTypeDecoder: Decoder[DataType] = Decoder.instance { c =>
    decodeOracleSpecific(c).orElse(CommonDataTypeCodecs.tryDecodeCommon(c))
  }

  // OracleColumnOptions codec
  given columnOptionsEncoder: Encoder[ColumnOptions] = deriveEncoder[OracleColumnOptions]
  given columnOptionsDecoder: Decoder[ColumnOptions] = deriveConfiguredDecoder[OracleColumnOptions]

  // OracleStorageClause codec
  given Encoder[OracleStorageClause] = deriveEncoder[OracleStorageClause]
  given Decoder[OracleStorageClause] = deriveDecoder[OracleStorageClause]

  // OracleCompressType codec
  given Encoder[OracleCompressType] = deriveConfiguredEncoder[OracleCompressType]
  given Decoder[OracleCompressType] = deriveConfiguredDecoder[OracleCompressType]

  // OracleParallelClause codec
  given Encoder[OracleParallelClause] = deriveEncoder[OracleParallelClause]
  given Decoder[OracleParallelClause] = deriveDecoder[OracleParallelClause]

  // OracleTableOptions codec
  given tableOptionsEncoder: Encoder[TableOptions] = deriveEncoder[OracleTableOptions]
  given tableOptionsDecoder: Decoder[TableOptions] = deriveConfiguredDecoder[OracleTableOptions]

  // OracleDropOptions codec
  given dropOptionsEncoder: Encoder[DropOptions] = deriveEncoder[OracleDropOptions]
  given dropOptionsDecoder: Decoder[DropOptions] = deriveConfiguredDecoder[OracleDropOptions]

  given createViewOptionsEncoder: Encoder[CreateViewOptions] = deriveEncoder[OracleCreateViewOptions]
  given createViewOptionsDecoder: Decoder[CreateViewOptions] = deriveConfiguredDecoder[OracleCreateViewOptions]

  given dropViewOptionsEncoder: Encoder[DropViewOptions] = deriveEncoder[OracleDropViewOptions]
  given dropViewOptionsDecoder: Decoder[DropViewOptions] = deriveConfiguredDecoder[OracleDropViewOptions]

  given indexOptionsEncoder: Encoder[IndexOptions] = deriveEncoder[OracleIndexOptions]
  given indexOptionsDecoder: Decoder[IndexOptions] = deriveConfiguredDecoder[OracleIndexOptions]

  given explainOptionsEncoder: Encoder[ExplainOptions] = deriveEncoder[OracleExplainOptions]
  given explainOptionsDecoder: Decoder[ExplainOptions] = deriveConfiguredDecoder[OracleExplainOptions]

  // Oracle-specific AlterTableAction codecs
  given addColumnsEncoder: Encoder[AddColumns[DataType, ColumnOptions]] = Encoder.instance { ac =>
    Json.obj(
      "type" -> Json.fromString("AddColumns"),
      "columns" -> ac.columns.asJson(using Encoder.encodeSeq(using columnEncoder))
    )
  }

  given addColumnsDecoder: Decoder[AddColumns[DataType, ColumnOptions]] = Decoder.instance { c =>
    c.get[Seq[Column[DataType, ColumnOptions]]]("columns")(using Decoder.decodeSeq(using columnDecoder))
      .map(cols => AddColumns(cols))
  }

  given modifyColumnsEncoder: Encoder[ModifyColumns[DataType, ColumnOptions]] = Encoder.instance { mc =>
    Json.obj(
      "type" -> Json.fromString("ModifyColumns"),
      "columns" -> mc.columns.asJson(using Encoder.encodeSeq(using columnEncoder))
    )
  }

  given modifyColumnsDecoder: Decoder[ModifyColumns[DataType, ColumnOptions]] = Decoder.instance { c =>
    c.get[Seq[Column[DataType, ColumnOptions]]]("columns")(using Decoder.decodeSeq(using columnDecoder))
      .map(cols => ModifyColumns(cols))
  }

  given dropColumnsEncoder: Encoder[DropColumns[DataType, ColumnOptions]] = Encoder.instance { dc =>
    Json.obj("type" -> Json.fromString("DropColumns"), "columnNames" -> dc.columnNames.asJson)
  }

  given dropColumnsDecoder: Decoder[DropColumns[DataType, ColumnOptions]] = Decoder.instance { c =>
    c.get[Seq[String]]("columnNames").map(names => DropColumns(names))
  }

  given addNamedConstraintEncoder: Encoder[AddNamedConstraint[DataType, ColumnOptions]] = Encoder.instance { anc =>
    Json.obj(
      "type" -> Json.fromString("AddNamedConstraint"),
      "name" -> Json.fromString(anc.name),
      "constraint" -> anc.constraint.asJson
    )
  }

  given addNamedConstraintDecoder: Decoder[AddNamedConstraint[DataType, ColumnOptions]] = Decoder.instance { c =>
    for {
      name <- c.get[String]("name")
      constraint <- c.get[TableConstraint]("constraint")
    } yield AddNamedConstraint(name, constraint)
  }

  // Combined AlterTableAction encoder/decoder for Oracle
  given oracleAlterTableActionEncoder: Encoder[AlterTableAction[DataType, ColumnOptions]] = Encoder.instance {
    case ac: AddColumn[DataType, ColumnOptions] => addColumnEncoder(ac)
    case dc: DropColumn[DataType, ColumnOptions] => dropColumnEncoder(dc)
    case mc: ModifyColumn[DataType, ColumnOptions] => modifyColumnEncoder(mc)
    case rc: RenameColumn[DataType, ColumnOptions] => renameColumnEncoder(rc)
    case ac: AddConstraint[DataType, ColumnOptions] => addConstraintEncoder(ac)
    case dc: DropConstraint[DataType, ColumnOptions] => dropConstraintEncoder(dc)
    case acs: AddColumns[DataType, ColumnOptions] => addColumnsEncoder(acs)
    case mcs: ModifyColumns[DataType, ColumnOptions] => modifyColumnsEncoder(mcs)
    case dcs: DropColumns[DataType, ColumnOptions] => dropColumnsEncoder(dcs)
    case anc: AddNamedConstraint[DataType, ColumnOptions] => addNamedConstraintEncoder(anc)
  }

  given oracleAlterTableActionDecoder: Decoder[AlterTableAction[DataType, ColumnOptions]] = Decoder.instance { c =>
    c.get[String]("type").flatMap {
      case "AddColumn" => addColumnDecoder(c)
      case "DropColumn" => dropColumnDecoder(c)
      case "ModifyColumn" => modifyColumnDecoder(c)
      case "RenameColumn" => renameColumnDecoder(c)
      case "AddConstraint" => addConstraintDecoder(c)
      case "DropConstraint" => dropConstraintDecoder(c)
      case "AddColumns" => addColumnsDecoder(c)
      case "ModifyColumns" => modifyColumnsDecoder(c)
      case "DropColumns" => dropColumnsDecoder(c)
      case "AddNamedConstraint" => addNamedConstraintDecoder(c)
      case other => Left(DecodingFailure(s"Unknown AlterTableAction type: $other", c.history))
    }
  }

  // AlterTable encoder/decoder for Oracle
  given alterTableEncoder
    : Encoder[AlterTable[DataType, ColumnOptions, TableOptions, AlterTableAction[DataType, ColumnOptions]]] =
    Encoder.instance { at =>
      Json.obj(
        "type" -> Json.fromString("AlterTable"),
        "tableName" -> Json.fromString(at.tableName),
        "actions" -> at.actions.asJson(using Encoder.encodeSeq(using oracleAlterTableActionEncoder))
      )
    }

  given alterTableDecoder
    : Decoder[AlterTable[DataType, ColumnOptions, TableOptions, AlterTableAction[DataType, ColumnOptions]]] =
    Decoder.instance { c =>
      for {
        tableName <- c.get[String]("tableName")
        actions <- c.get[Seq[AlterTableAction[DataType, ColumnOptions]]]("actions")(using
          Decoder.decodeSeq(using oracleAlterTableActionDecoder)
        )
      } yield AlterTable(tableName, actions)
    }

  // =========================================================================
  // Oracle Statement encoders
  // =========================================================================

  given Encoder[Grant] = Encoder.instance { g =>
    Json.obj(
      "type" -> Json.fromString("Grant"),
      "privileges" -> g.privileges.asJson,
      "onObject" -> g.onObject.asJson,
      "toUsers" -> g.toUsers.asJson,
      "withGrantOption" -> Json.fromBoolean(g.withGrantOption)
    )
  }

  given Encoder[Revoke] = Encoder.instance { r =>
    Json.obj(
      "type" -> Json.fromString("Revoke"),
      "privileges" -> r.privileges.asJson,
      "onObject" -> r.onObject.asJson,
      "fromUsers" -> r.fromUsers.asJson
    )
  }

  given Encoder[OracleCreateSynonym] = Encoder.instance { s =>
    Json.obj(
      "type" -> Json.fromString("OracleCreateSynonym"),
      "name" -> s.name.asJson,
      "orReplace" -> Json.fromBoolean(s.orReplace),
      "target" -> s.target.asJson
    )
  }

  given Encoder[OracleDropSynonym] = Encoder.instance { s =>
    Json.obj(
      "type" -> Json.fromString("OracleDropSynonym"),
      "name" -> s.name.asJson,
      "ifExists" -> Json.fromBoolean(s.ifExists)
    )
  }

  given Encoder[OracleRename] = Encoder.instance { r =>
    Json.obj("type" -> Json.fromString("OracleRename"), "oldName" -> r.oldName.asJson, "newName" -> r.newName.asJson)
  }

  given Encoder[Prompt] = Encoder.instance { p =>
    Json.obj("type" -> Json.fromString("Prompt"), "message" -> Json.fromString(p.message))
  }

  given Encoder[Commit] = Encoder.instance { _ =>
    Json.obj("type" -> Json.fromString("Commit"))
  }

  given Encoder[SetDefineOff] = Encoder.instance { _ =>
    Json.obj("type" -> Json.fromString("SetDefineOff"))
  }

  given Encoder[SetScanOff] = Encoder.instance { _ =>
    Json.obj("type" -> Json.fromString("SetScanOff"))
  }

  given Encoder[SetScanOn] = Encoder.instance { _ =>
    Json.obj("type" -> Json.fromString("SetScanOn"))
  }

  given Encoder[PlSqlBlock] = Encoder.instance { p =>
    Json.obj("type" -> Json.fromString("PlSqlBlock"), "content" -> Json.fromString(p.content))
  }

  given Encoder[PlSqlForLoop] = Encoder.instance { p =>
    Json.obj(
      "type" -> Json.fromString("PlSqlForLoop"),
      "variable" -> p.variable.asJson,
      "query" -> p.query.asJson,
      "content" -> Json.fromString(p.content)
    )
  }

  given Encoder[PlSqlWhileLoop] = Encoder.instance { p =>
    Json.obj(
      "type" -> Json.fromString("PlSqlWhileLoop"),
      "condition" -> Json.fromString(p.condition),
      "content" -> Json.fromString(p.content)
    )
  }

  given Encoder[OracleColumnComment] =
    deriveEncoder[OracleColumnComment].mapJson(_.mapObject(_.add("type", Json.fromString("OracleColumnComment"))))
  given Decoder[OracleColumnComment] = deriveDecoder[OracleColumnComment]

  given Encoder[OracleTableComment] =
    deriveEncoder[OracleTableComment].mapJson(_.mapObject(_.add("type", Json.fromString("OracleTableComment"))))
  given Decoder[OracleTableComment] = deriveDecoder[OracleTableComment]

  given Encoder[OracleAlterView] = Encoder.instance { av =>
    Json.obj("type" -> Json.fromString("OracleAlterView"), "name" -> av.name.asJson, "action" -> av.action.asJson)
  }

  given Encoder[StorageClause] = Encoder.instance { sc =>
    Json.obj(
      "type" -> Json.fromString("StorageClause"),
      "initial" -> sc.initial.asJson,
      "next" -> sc.next.asJson,
      "minExtents" -> sc.minExtents.asJson,
      "maxExtents" -> sc.maxExtents.asJson,
      "pctIncrease" -> sc.pctIncrease.asJson,
      "bufferPool" -> sc.bufferPool.asJson
    )
  }

  given Encoder[UserQuota] = Encoder.instance { q =>
    Json.obj("tablespace" -> Json.fromString(q.tablespace), "quota" -> q.quota.asJson)
  }

  given Decoder[UserQuota] = Decoder.instance { c =>
    for {
      tablespace <- c.get[String]("tablespace")
      quota <- c.get[Option[String]]("quota")
    } yield UserQuota(tablespace, quota)
  }

  given Encoder[CreateTablespace] = Encoder.instance { ct =>
    Json.obj(
      "type" -> Json.fromString("CreateTablespace"),
      "name" -> Json.fromString(ct.name),
      "datafile" -> ct.datafile.asJson,
      "size" -> ct.size.asJson,
      "autoextend" -> Json.fromBoolean(ct.autoextend)
    )
  }

  given Decoder[CreateTablespace] = Decoder.instance { c =>
    for {
      name <- c.get[String]("name")
      datafile <- c.get[Option[String]]("datafile")
      size <- c.get[Option[String]]("size")
      autoextend <- c.getOrElse[Boolean]("autoextend")(true)
    } yield CreateTablespace(name, datafile, size, autoextend)
  }

  given Encoder[CreateUser] = Encoder.instance { cu =>
    Json.obj(
      "type" -> Json.fromString("CreateUser"),
      "name" -> Json.fromString(cu.name),
      "identifiedBy" -> Json.fromString(cu.identifiedBy),
      "defaultTablespace" -> cu.defaultTablespace.asJson,
      "quotas" -> cu.quotas.asJson,
      "roles" -> cu.roles.asJson
    )
  }

  given Decoder[CreateUser] = Decoder.instance { c =>
    for {
      name <- c.get[String]("name")
      identifiedBy <- c.get[String]("identifiedBy")
      defaultTablespace <- c.get[Option[String]]("defaultTablespace")
      quotas <- c.getOrElse[Seq[UserQuota]]("quotas")(Seq.empty)
      roles <- c.getOrElse[Seq[String]]("roles")(Seq("CONNECT", "RESOURCE"))
    } yield CreateUser(name, identifiedBy, defaultTablespace, quotas, roles)
  }

  given Encoder[ColumnModification] = Encoder.instance { cm =>
    Json.obj("column" -> cm.column.asJson, "dataType" -> cm.dataType.asJson, "annotations" -> cm.annotations.asJson)
  }

  given Encoder[AlterTableModify] = Encoder.instance { atm =>
    Json.obj("type" -> Json.fromString("AlterTableModify"), "modifications" -> atm.modifications.asJson)
  }

  given Encoder[AlterTableAdd] = Encoder.instance { ata =>
    Json.obj(
      "type" -> Json.fromString("AlterTableAdd"),
      "columns" -> ata.columns.asJson,
      "annotations" -> ata.annotations.asJson
    )
  }

  // OracleStatement encoder
  given oracleStatementEncoder: Encoder[OracleStatement] = Encoder.instance {
    case g: Grant => g.asJson
    case r: Revoke => r.asJson
    case cs: OracleCreateSynonym => cs.asJson
    case ds: OracleDropSynonym => ds.asJson
    case rn: OracleRename => rn.asJson
    case p: Prompt => p.asJson
    case c: Commit => c.asJson
    case sdo: SetDefineOff => sdo.asJson
    case sso: SetScanOff => sso.asJson
    case sson: SetScanOn => sson.asJson
    case psb: PlSqlBlock => psb.asJson
    case pfl: PlSqlForLoop => pfl.asJson
    case pwl: PlSqlWhileLoop => pwl.asJson
    case cc: OracleColumnComment => cc.asJson
    case tc: OracleTableComment => tc.asJson
    case oav: OracleAlterView => oav.asJson
    case ct: CreateTablespace => ct.asJson
    case cu: CreateUser => cu.asJson
  }

  // Override hook to handle Oracle-specific statements
  override protected def encodeDialectSpecific(stmt: Statement): Json = stmt match
    case os: OracleStatement => os.asJson(using oracleStatementEncoder)
    case other => super.encodeDialectSpecific(other)

  // Override hook to handle Oracle-specific alter table actions
  override protected def encodeDialectSpecificAction(action: AlterTableAction[DataType, ColumnOptions]): Json =
    action match
      case m: AlterTableModify => summon[Encoder[AlterTableModify]].apply(m)
      case a: AlterTableAdd => summon[Encoder[AlterTableAdd]].apply(a)
      case other => super.encodeDialectSpecificAction(other)

  // Override hook to handle Oracle-specific statement decoding
  override protected def decodeDialectSpecific(c: io.circe.HCursor, typeName: String): Decoder.Result[Statement] =
    import sequala.schema.ast.Name
    typeName match
      case "Grant" =>
        for {
          privileges <- c.get[Seq[String]]("privileges")
          onObject <- c.get[Name]("onObject")
          toUsers <- c.get[Seq[Name]]("toUsers")
          withGrantOption <- c.getOrElse[Boolean]("withGrantOption")(false)
        } yield Grant(privileges, onObject, toUsers, withGrantOption)
      case "Revoke" =>
        for {
          privileges <- c.get[Seq[String]]("privileges")
          onObject <- c.get[Name]("onObject")
          fromUsers <- c.get[Seq[Name]]("fromUsers")
        } yield Revoke(privileges, onObject, fromUsers)
      case "Comment" | "OracleColumnComment" =>
        summon[Decoder[OracleColumnComment]].tryDecode(c)
      case "TableComment" | "OracleTableComment" =>
        summon[Decoder[OracleTableComment]].tryDecode(c)
      case "CreateTablespace" =>
        summon[Decoder[CreateTablespace]].tryDecode(c)
      case "CreateUser" =>
        summon[Decoder[CreateUser]].tryDecode(c)
      case other => super.decodeDialectSpecific(c, other)
