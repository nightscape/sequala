package sequala.common.alter

import sequala.common.expression.ToSql

sealed abstract class AlterViewAction extends ToSql

case class Materialize(add: Boolean) extends AlterViewAction {
  override def toSql = (if (add) { "" }
                        else { "DROP " }) + "MATERIALIZE"
}
