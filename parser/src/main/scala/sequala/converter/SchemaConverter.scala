package sequala.converter

import sequala.common.statement.{
  ColumnDefaultValue,
  ColumnDefinition,
  ColumnIsNotNullable,
  ColumnIsPrimaryKey,
  CreateTable as ParserCreateTable,
  TableForeignKey,
  TableIndexOn,
  TablePrimaryKey,
  TableUnique
}
import sequala.common.expression.PrimitiveValue
import sequala.common.Name
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
    parserTable: ParserCreateTable
  ): CreateTable[CommonDataType, NoColumnOptions.type, NoTableOptions.type] =
    val columnPkNames = parserTable.columns
      .filter(_.annotations.exists(_.isInstanceOf[ColumnIsPrimaryKey]))
      .map(_.name.name)

    val tablePkNames = parserTable.annotations.collect { case TablePrimaryKey(cols) =>
      cols.map(_.name)
    }.flatten

    val allPkNames = (columnPkNames ++ tablePkNames).distinct

    val primaryKey =
      if allPkNames.nonEmpty then Some(PrimaryKey(None, allPkNames))
      else None

    val indexes = parserTable.annotations.collect { case TableIndexOn(cols) =>
      Index(None, cols.map(n => IndexColumn(n.name)))
    }

    val uniques = parserTable.annotations.collect { case TableUnique(cols) =>
      Unique(None, cols.map(_.name))
    }

    val foreignKeys = parserTable.annotations.collect { case fk: TableForeignKey =>
      ForeignKey(
        name = fk.name.map(_.name),
        columns = fk.columns.map(_.name),
        refTable = fk.refTable.name,
        refColumns = fk.refColumns.map(_.name),
        onUpdate = fk.onUpdate.getOrElse(NoAction),
        onDelete = fk.onDelete.getOrElse(NoAction)
      )
    }

    val columns = parserTable.columns.map(convertColumnDefinition)

    val table = Table(
      name = parserTable.name.name,
      columns = columns,
      primaryKey = primaryKey,
      indexes = indexes,
      foreignKeys = foreignKeys,
      checks = Seq.empty,
      uniques = uniques,
      options = NoTableOptions
    )

    CreateTable(table = table, orReplace = parserTable.orReplace, ifNotExists = false)

  def convertColumnDefinition(colDef: ColumnDefinition): Column[CommonDataType, NoColumnOptions.type] =
    val dataType = convertDataType(colDef.t, colDef.args)
    val nullable = !colDef.annotations.exists(_.isInstanceOf[ColumnIsNotNullable])
    val default = colDef.annotations.collectFirst { case ColumnDefaultValue(expr) =>
      expr.toSql
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
    val intArgs = args.collect { case sequala.common.expression.LongPrimitive(v) =>
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
