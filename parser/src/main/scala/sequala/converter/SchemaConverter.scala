package sequala.converter

import sequala.schema.statement.{
  ColumnDefaultValue,
  ColumnDefinition,
  ColumnIsNotNullable,
  ColumnIsPrimaryKey,
  TableAnnotation,
  TableForeignKey,
  TableIndexOn,
  TablePrimaryKey,
  TableUnique
}
import sequala.schema.ast.Name
import sequala.schema.ast.{Expression, LongPrimitive, PrimitiveValue}
import sequala.schema.{
  Column,
  CommonDataType,
  CreateTable,
  Decimal,
  DoublePrecision,
  ForeignKey,
  Index,
  IndexColumn,
  NoAction,
  NoColumnOptions,
  NoTableOptions,
  Numeric,
  PrimaryKey,
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
  Table,
  Unique,
  VarChar
}

object SchemaConverter:

  def convertCreateTable(
    name: String,
    schema: Option[String],
    orReplace: Boolean,
    columns: Seq[ColumnDefinition],
    annotations: Seq[TableAnnotation]
  ): CreateTable[CommonDataType, NoColumnOptions.type, NoTableOptions.type] =
    val columnPkNames = columns
      .filter(_.annotations.exists(_.isInstanceOf[ColumnIsPrimaryKey]))
      .map(_.name.name)

    val tablePks = annotations.collect { case pk: TablePrimaryKey => pk }
    val columnPrimaryKey =
      if columnPkNames.nonEmpty then Some(PrimaryKey(None, columnPkNames))
      else None

    val primaryKey = tablePks.headOption
      .map(pk => PrimaryKey(pk.name.map(_.name), pk.columns.map(_.name)))
      .orElse(columnPrimaryKey)

    val indexes = annotations.collect { case TableIndexOn(columns) =>
      Index(None, columns.map(n => IndexColumn(n.name)))
    }

    val uniques = annotations.collect { case u: TableUnique =>
      Unique(u.name.map(_.name), u.columns.map(_.name))
    }

    val foreignKeys = annotations.collect { case fk: TableForeignKey =>
      ForeignKey(
        name = fk.name.map(_.name),
        columns = fk.columns.map(_.name),
        refTable = fk.refTable.name,
        refColumns = fk.refColumns.map(_.name),
        onUpdate = fk.onUpdate.getOrElse(NoAction),
        onDelete = fk.onDelete.getOrElse(NoAction)
      )
    }

    val schemaColumns = columns.map(convertColumnDefinition)

    val table = Table(
      name = name,
      columns = schemaColumns,
      primaryKey = primaryKey,
      indexes = indexes,
      foreignKeys = foreignKeys,
      checks = Seq.empty,
      uniques = uniques,
      options = NoTableOptions,
      comment = None,
      schema = schema
    )

    CreateTable(table = table, orReplace = orReplace, ifNotExists = false)

  def convertColumnDefinition(colDef: ColumnDefinition): Column[CommonDataType, NoColumnOptions.type] =
    val dataType = convertDataType(colDef.t, colDef.args)
    val nullable = !colDef.annotations.exists(_.isInstanceOf[ColumnIsNotNullable])
    val default = colDef.annotations.collectFirst { case ColumnDefaultValue(expr) =>
      import sequala.common.renderer.ParserSqlRenderers.given
      import sequala.schema.{SqlFormatConfig, SqlRenderer}
      given SqlFormatConfig = sequala.schema.SqlFormatConfig.Compact
      // Render parser Expression using parser renderer (still needed during transition)
      // ParserSqlRenderers provides SqlRenderer[Expression] for parser AST
      summon[SqlRenderer[Expression]].toSql(expr)
    }

    Column(
      name = colDef.name.name,
      dataType = dataType,
      nullable = nullable,
      default = default,
      options = NoColumnOptions
    )

  def convertDataType(typeName: Name, args: Seq[PrimitiveValue]): CommonDataType =
    val name = typeName.name.toUpperCase
    val intArgs = args.collect { case LongPrimitive(v) =>
      v.toInt
    }

    name match
      case "VARCHAR" | "VARCHAR2" | "NVARCHAR" | "NVARCHAR2" | "CHARACTER VARYING" =>
        VarChar(intArgs.headOption.getOrElse(255))
      case "CHAR" | "CHARACTER" | "NCHAR" =>
        SqlChar(intArgs.headOption.getOrElse(1))
      case "INT" | "INTEGER" =>
        SqlInteger
      case "BIGINT" =>
        SqlBigInt
      case "SMALLINT" =>
        SmallInt
      case "DECIMAL" =>
        val precision = intArgs.headOption.getOrElse(18)
        val scale = intArgs.lift(1).getOrElse(0)
        Decimal(precision, scale)
      case "NUMERIC" | "NUMBER" =>
        val precision = intArgs.headOption.getOrElse(18)
        val scale = intArgs.lift(1).getOrElse(0)
        Numeric(precision, scale)
      case "REAL" | "FLOAT" =>
        Real
      case "DOUBLE" | "DOUBLE PRECISION" =>
        DoublePrecision
      case "BOOLEAN" | "BOOL" =>
        SqlBoolean
      case "DATE" =>
        SqlDate
      case "TIME" =>
        SqlTime(intArgs.headOption, withTimeZone = false)
      case "TIMETZ" | "TIME WITH TIME ZONE" =>
        SqlTime(intArgs.headOption, withTimeZone = true)
      case "TIMESTAMP" =>
        SqlTimestamp(intArgs.headOption, withTimeZone = false)
      case "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" =>
        SqlTimestamp(intArgs.headOption, withTimeZone = true)
      case "TEXT" =>
        SqlText
      case "BLOB" | "BYTEA" =>
        SqlBlob
      case "CLOB" =>
        SqlClob
      case _ =>
        VarChar(255)
