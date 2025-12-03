package sequala.schema.statement

import sequala.schema.ast.Expression

sealed abstract class ColumnAnnotation

case class ColumnIsPrimaryKey() extends ColumnAnnotation
case class ColumnIsNotNullable() extends ColumnAnnotation
case class ColumnIsNullable() extends ColumnAnnotation
case class ColumnDefaultValue(v: Expression) extends ColumnAnnotation
case class ColumnIsInvisible() extends ColumnAnnotation

// Oracle constraint state modifiers
case class ColumnConstraintEnable() extends ColumnAnnotation
case class ColumnConstraintDisable() extends ColumnAnnotation
case class ColumnConstraintValidate() extends ColumnAnnotation
case class ColumnConstraintNovalidate() extends ColumnAnnotation
