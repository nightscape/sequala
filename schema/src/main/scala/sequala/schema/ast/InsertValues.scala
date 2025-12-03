package sequala.schema.ast

import sequala.schema.ast.{Expression, SelectBody}

sealed trait InsertValues

case class ExplicitInsert(values: Seq[Seq[Expression]]) extends InsertValues

case class SelectInsert(query: SelectBody) extends InsertValues
