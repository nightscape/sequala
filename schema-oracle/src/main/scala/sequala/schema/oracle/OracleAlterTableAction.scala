package sequala.schema.oracle

import sequala.schema.{AlterTableAction, Column, ColumnOptions, DataType, IdentifierQuoting, TableConstraint}
import sequala.schema.ast.SqlComment

/** Oracle-specific ALTER TABLE actions that extend the base AlterTableAction trait.
  *
  * These support Oracle's batch operations which allow multiple columns/constraints in a single statement.
  */

/** ADD multiple columns in a single statement: ALTER TABLE t ADD (col1 INT, col2 VARCHAR(100)) */
case class AddColumns[DT <: DataType, CO <: ColumnOptions](
  columns: Seq[Column[DT, CO]],
  comment: Seq[SqlComment] = Seq.empty
) extends AlterTableAction[DT, CO](comment)

/** MODIFY multiple columns in a single statement: ALTER TABLE t MODIFY (col1 INT, col2 NOT NULL) */
case class ModifyColumns[DT <: DataType, CO <: ColumnOptions](
  columns: Seq[Column[DT, CO]],
  comment: Seq[SqlComment] = Seq.empty
) extends AlterTableAction[DT, CO](comment)

/** DROP multiple columns in a single statement: ALTER TABLE t DROP (col1, col2) */
case class DropColumns[DT, CO <: ColumnOptions](columnNames: Seq[String], comment: Seq[SqlComment] = Seq.empty)
    extends AlterTableAction[DT, CO](comment)

/** ADD CONSTRAINT with explicit name: ALTER TABLE t ADD CONSTRAINT pk_name PRIMARY KEY (id) */
case class AddNamedConstraint[DT, CO <: ColumnOptions](
  name: String,
  constraint: TableConstraint,
  comment: Seq[SqlComment] = Seq.empty
) extends AlterTableAction[DT, CO](comment)
