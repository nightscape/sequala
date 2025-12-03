package sequala.common.statement

import sequala.common.Name
import sequala.common.select.SelectBody
import sequala.common.expression.ToSql

case class WithClause(body: SelectBody, name: Name) extends ToSql {
  override def toSql = s"${name.toSql} AS (${body.toSql})"
}
