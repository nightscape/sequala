package sequala.common.statement

import sequala.common.expression.Expression
import sequala.common.select.SelectBody
import sequala.common.expression.ToSql

sealed abstract class InsertValues extends ToSql

case class ExplicitInsert(values: Seq[Seq[Expression]]) extends InsertValues {
  override def toSql =
    "VALUES " + values.map("(" + _.map(_.toSql).mkString(", ") + ")").mkString(", ")
}
case class SelectInsert(query: SelectBody) extends InsertValues {
  override def toSql = query.toSql
}
