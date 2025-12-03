package sequala.schema.statement

import sequala.schema.ast.{Name, PrimitiveValue}

case class ColumnDefinition(
  name: Name,
  t: Name,
  args: Seq[PrimitiveValue] = Seq(),
  annotations: Seq[ColumnAnnotation] = Seq()
)
