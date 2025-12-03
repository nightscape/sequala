package sequala.converter

import sequala.common.statement.{CreateIndex, CreateTableStatement, Statement}
import sequala.schema.{CommonDataType, Index, IndexColumn, NoColumnOptions, NoTableOptions, Table}

object SchemaBuilder:

  type GenericTable = Table[CommonDataType, NoColumnOptions.type, NoTableOptions.type]

  def fromStatements(statements: Seq[Statement]): Seq[GenericTable] =
    val tables = statements.collect { case cts: CreateTableStatement =>
      cts.schemaTable.table
    }

    val standaloneIndexes = statements.collect { case ci: CreateIndex =>
      (ci.table.name, convertCreateIndex(ci))
    }

    val indexesByTable = standaloneIndexes.groupBy(_._1).view.mapValues(_.map(_._2)).toMap

    tables.map { table =>
      val additionalIndexes = indexesByTable.getOrElse(table.name, Seq.empty)
      if additionalIndexes.isEmpty then table
      else table.copy(indexes = table.indexes ++ additionalIndexes)
    }

  private def convertCreateIndex(ci: CreateIndex): Index =
    Index(name = Some(ci.name.name), columns = ci.columns.map(c => IndexColumn(c.name)), unique = ci.unique)
