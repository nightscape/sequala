package sequala.schema.oracle

import sequala.schema.{AlterTableAction, Column, ColumnOptions, DataType, IdentifierQuoting, TableConstraint}

/** Oracle-specific ALTER TABLE actions that extend the base AlterTableAction trait.
  *
  * These support Oracle's batch operations which allow multiple columns/constraints in a single statement.
  */

/** ADD multiple columns in a single statement: ALTER TABLE t ADD (col1 INT, col2 VARCHAR(100)) */
case class AddColumns[DT <: DataType, CO <: ColumnOptions](columns: Seq[Column[DT, CO]])
    extends AlterTableAction[DT, CO]:
  def toClause: String =
    val colDefs = columns.map { col =>
      val nullStr = if col.nullable then "" else " NOT NULL"
      val defaultStr = col.default.map(d => s" DEFAULT $d").getOrElse("")
      s"${quote(col.name)} ${col.dataType.toSql}$defaultStr$nullStr"
    }
    s"ADD (${colDefs.mkString(", ")})"

/** MODIFY multiple columns in a single statement: ALTER TABLE t MODIFY (col1 INT, col2 NOT NULL) */
case class ModifyColumns[DT <: DataType, CO <: ColumnOptions](columns: Seq[Column[DT, CO]])
    extends AlterTableAction[DT, CO]:
  def toClause: String =
    val colDefs = columns.map { col =>
      val nullStr = if col.nullable then "" else " NOT NULL"
      val defaultStr = col.default.map(d => s" DEFAULT $d").getOrElse("")
      s"${quote(col.name)} ${col.dataType.toSql}$defaultStr$nullStr"
    }
    s"MODIFY (${colDefs.mkString(", ")})"

/** DROP multiple columns in a single statement: ALTER TABLE t DROP (col1, col2) */
case class DropColumns[DT, CO <: ColumnOptions](columnNames: Seq[String]) extends AlterTableAction[DT, CO]:
  def toClause: String = s"DROP (${columnNames.map(quote).mkString(", ")})"

/** ADD CONSTRAINT with explicit name: ALTER TABLE t ADD CONSTRAINT pk_name PRIMARY KEY (id) */
case class AddNamedConstraint[DT, CO <: ColumnOptions](name: String, constraint: TableConstraint)
    extends AlterTableAction[DT, CO]:
  def toClause: String = s"ADD CONSTRAINT ${quote(name)} ${constraint.toClause}"
