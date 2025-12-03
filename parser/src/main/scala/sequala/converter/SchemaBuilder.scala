package sequala.converter

import sequala.schema.{
  AnsiDialect,
  ColumnCommentStatement,
  CreateIndex,
  CreateTable,
  DbDialect,
  Index,
  IndexColumn,
  Statement,
  Table,
  TableCommentStatement
}

/** Trait for extracting typed tables from parsed statements.
  *
  * Each dialect provides its own SchemaBuilder instance that preserves dialect-specific types. This avoids lossy type
  * conversions that can lose information like the `schema` field.
  */
trait SchemaBuilder { self: DbDialect =>

  type DialectTable = Table[DataType, ColumnOptions, TableOptions]
  type DialectCreateTable = CreateTable[DataType, ColumnOptions, TableOptions]

  def fromStatements(statements: Seq[Statement]): Seq[DialectTable] =
    val tables = statements.collect { case ct: DialectCreateTable @unchecked =>
      ct.table
    }

    val standaloneIndexes = statements.collect { case ci: CreateIndex[?] =>
      (ci.tableName, convertCreateIndex(ci))
    }

    val indexesByTable = standaloneIndexes.groupBy(_._1).view.mapValues(_.map(_._2)).toMap

    val tableComments = SchemaBuilder.extractTableComments(statements)
    val columnComments = SchemaBuilder.extractColumnComments(statements)

    tables.map { table =>
      // Look up indexes by both name and qualifiedName, but avoid duplicates when they're the same
      val indexesByName = indexesByTable.getOrElse(table.name, Seq.empty)
      val indexesByQualifiedName =
        if table.name == table.qualifiedName then Seq.empty
        else indexesByTable.getOrElse(table.qualifiedName, Seq.empty)
      val additionalIndexes = indexesByName ++ indexesByQualifiedName

      val tableWithIndexes =
        if additionalIndexes.isEmpty then table
        else table.copy(indexes = table.indexes ++ additionalIndexes)

      SchemaBuilder.applyComments(tableWithIndexes, tableComments, columnComments)
    }

  private def convertCreateIndex(ci: CreateIndex[?]): Index =
    Index(
      name = Some(ci.name),
      columns = ci.columns.map(c => IndexColumn(c.name)),
      unique = ci.unique,
      where = ci.options.where
    )
}

object SchemaBuilder:

  def applyComments[DT, CO <: sequala.schema.ColumnOptions, TO <: sequala.schema.TableOptions](
    table: Table[DT, CO, TO],
    tableComments: Map[String, String],
    columnComments: Map[(String, String), String]
  ): Table[DT, CO, TO] =
    val tableComment = tableComments
      .get(table.name)
      .orElse(tableComments.get(table.name.toUpperCase))
      .orElse(tableComments.get(table.qualifiedName))
      .orElse(tableComments.get(table.qualifiedName.toUpperCase))

    val columnsWithComments = table.columns.map { col =>
      val colComment = columnComments
        .get((table.name, col.name))
        .orElse(columnComments.get((table.name.toUpperCase, col.name.toUpperCase)))
        .orElse(columnComments.get((table.qualifiedName, col.name)))
        .orElse(columnComments.get((table.qualifiedName.toUpperCase, col.name.toUpperCase)))
      colComment match
        case Some(comment) => col.copy(comment = Some(comment))
        case None => col
    }

    table.copy(columns = columnsWithComments, comment = tableComment)

  def extractTableComments(statements: Seq[Statement]): Map[String, String] =
    statements.collect { case tcs: TableCommentStatement =>
      tcs.tableName -> tcs.comment
    }.toMap

  def extractColumnComments(statements: Seq[Statement]): Map[(String, String), String] =
    statements.collect { case ccs: ColumnCommentStatement =>
      (ccs.tableName, ccs.columnName) -> ccs.comment
    }.toMap

/** Default SchemaBuilder for ANSI/Generic dialect */
object AnsiSchemaBuilder extends SchemaBuilder with AnsiDialect
