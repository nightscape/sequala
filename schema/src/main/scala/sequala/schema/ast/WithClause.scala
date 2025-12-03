package sequala.schema.ast

import sequala.schema.ast.SelectBody

case class WithClause(name: Name, body: SelectBody)
