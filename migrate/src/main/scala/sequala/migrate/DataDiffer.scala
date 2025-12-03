package sequala.migrate

import sequala.schema.{Insert, Statement, Table}
import sequala.schema.ast.{ExplicitInsert, Expression, Name}

case class Row(table: Name, columns: Seq[Name], values: Map[String, Expression])

object DataDiffer:

  case class TableData(tableName: Name, columns: Seq[Name], rows: Seq[Row])

  def parseInserts(statements: Seq[Statement]): Map[String, TableData] =
    val inserts = statements.collect { case Insert(table, Some(cols), ExplicitInsert(valueRows), _) =>
      val rows = valueRows.map { row =>
        val valueMap = cols
          .zip(row)
          .map { case (col, expr) =>
            normalizeColumnName(col.name) -> expr
          }
          .toMap
        Row(table, cols, valueMap)
      }
      (table, cols, rows)
    }

    inserts
      .groupBy { case (table, _, _) => normalizeTableName(table) }
      .map { case (tableName, entries) =>
        val (table, cols, _) = entries.head
        val allRows = entries.flatMap(_._3)
        tableName -> TableData(table, cols, allRows)
      }

  def resolveKeyColumns(
    tableName: String,
    options: DataDiffOptions,
    ddlTables: Map[String, Table[?, ?, ?]]
  ): Either[String, Seq[String]] =
    options.keyColumns match
      case Some(keys) => Right(keys)
      case None =>
        ddlTables.get(tableName.toUpperCase).orElse(ddlTables.get(tableName)) match
          case Some(table) =>
            table.primaryKey match
              case Some(pk) => Right(pk.columns)
              case None => Left(s"No primary key found for table $tableName")
          case None =>
            Left(s"No key columns specified and table $tableName not found in DDL")

  def rowDiffable(keyColumns: Seq[String], generateDeletes: Boolean): Diffable[Seq[Expression], Row, DataDiffOp] =
    val normalizedKeys = keyColumns.map(_.toUpperCase)

    new Diffable[Seq[Expression], Row, DataDiffOp]:
      def extractKey(r: Row): Seq[Expression] =
        normalizedKeys.map(k => r.values(k))

      def createOp(r: Row): Seq[DataDiffOp] =
        Seq(InsertRow(r.table, r.columns, r.columns.map(c => r.values(normalizeColumnName(c.name)))))

      def deleteOp(r: Row): Seq[DataDiffOp] =
        if generateDeletes then Seq(DeleteRow(r.table, normalizedKeys.map(k => Name(k) -> r.values(k))))
        else Seq.empty

      def modifyOp(from: Row, to: Row): Seq[DataDiffOp] =
        val changedCols = to.columns.filter { col =>
          val k = normalizeColumnName(col.name)
          !normalizedKeys.contains(k) && from.values.get(k) != to.values.get(k)
        }
        if changedCols.isEmpty then Seq.empty
        else
          Seq(
            UpdateRow(
              to.table,
              normalizedKeys.map(k => Name(k) -> to.values(k)),
              changedCols.map(c => c -> to.values(normalizeColumnName(c.name)))
            )
          )

  def diff(
    from: Map[String, TableData],
    to: Map[String, TableData],
    options: DataDiffOptions,
    ddlTables: Map[String, Table[?, ?, ?]] = Map.empty
  ): Either[String, Seq[DataDiffOp]] =
    val allTables = (from.keySet ++ to.keySet).toSeq.sorted

    val results = allTables.map { tableName =>
      resolveKeyColumns(tableName, options, ddlTables).map { keyColumns =>
        val fromRows = from.get(tableName).map(_.rows).getOrElse(Seq.empty)
        val toRows = to.get(tableName).map(_.rows).getOrElse(Seq.empty)

        given Diffable[Seq[Expression], Row, DataDiffOp] = rowDiffable(keyColumns, options.generateDeletes)
        Differ.diff(fromRows, toRows)
      }
    }

    val errors = results.collect { case Left(e) => e }
    if errors.nonEmpty then Left(errors.mkString("; "))
    else Right(results.collect { case Right(ops) => ops }.flatten)

  private def normalizeColumnName(name: String): String = name.toUpperCase

  private def normalizeTableName(name: Name): String =
    name.name.toUpperCase
