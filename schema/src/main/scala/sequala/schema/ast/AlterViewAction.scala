package sequala.schema.ast

sealed trait AlterViewAction

case class Materialize(add: Boolean) extends AlterViewAction
