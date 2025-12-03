package sequala.schema.ast

/** Schema-level AST for SQL expressions, mirroring parser AST structure */
sealed abstract class Expression:
  def needsParenthesis: Boolean
  def children: Seq[Expression]
  def rebuild(newChildren: Seq[Expression]): Expression

object Expression:
  def escapeString(s: String): String = s.replaceAll("'", "''")

sealed abstract class PrimitiveValue extends Expression:
  def needsParenthesis = false
  def children: Seq[Expression] = Seq()
  def rebuild(newChildren: Seq[Expression]): Expression = this

case class LongPrimitive(v: Long) extends PrimitiveValue
case class DoublePrimitive(v: Double) extends PrimitiveValue
case class StringPrimitive(v: String) extends PrimitiveValue
case class BooleanPrimitive(v: Boolean) extends PrimitiveValue
case class NullPrimitive() extends PrimitiveValue

/** A bare identifier used as an expression (e.g., SYSDATE, CURRENT_TIMESTAMP) */
case class Identifier(name: Name) extends Expression:
  def needsParenthesis = false
  def children: Seq[Expression] = Seq()
  def rebuild(newChildren: Seq[Expression]): Expression = this

/** A column reference 'Table.Col' */
case class Column(column: Name, table: Option[Name] = None) extends Expression:
  def needsParenthesis = false
  def children: Seq[Expression] = Seq()
  def rebuild(newChildren: Seq[Expression]): Expression = this

/** Binary arithmetic expression */
case class Arithmetic(lhs: Expression, op: Arithmetic.Op, rhs: Expression) extends Expression:
  def needsParenthesis = true
  def children: Seq[Expression] = Seq(lhs, rhs)
  def rebuild(c: Seq[Expression]): Expression = Arithmetic(c(0), op, c(1))

object Arithmetic:
  enum Op:
    case Add, Sub, Mult, Div, And, Or, BitAnd, BitOr, ShiftLeft, ShiftRight

  def opString(v: Op): String = v match
    case Op.Add => "+"
    case Op.Sub => "-"
    case Op.Mult => "*"
    case Op.Div => "/"
    case Op.BitAnd => "&"
    case Op.BitOr => "|"
    case Op.ShiftLeft => "<<"
    case Op.ShiftRight => ">>"
    case Op.And => "AND"
    case Op.Or => "OR"

/** Comparison expression */
case class Comparison(lhs: Expression, op: Comparison.Op, rhs: Expression) extends Expression:
  def needsParenthesis = true
  def children: Seq[Expression] = Seq(lhs, rhs)
  def rebuild(c: Seq[Expression]): Expression = Comparison(c(0), op, c(1))

object Comparison:
  enum Op:
    case Eq, Neq, Gt, Lt, Gte, Lte, Like, NotLike, RLike, NotRLike

  def opString(v: Op): String = v match
    case Op.Eq => "="
    case Op.Neq => "<>"
    case Op.Gt => ">"
    case Op.Gte => ">="
    case Op.Lt => "<"
    case Op.Lte => "<="
    case Op.Like => "LIKE"
    case Op.NotLike => "NOT LIKE"
    case Op.RLike => "RLIKE"
    case Op.NotRLike => "NOT RLIKE"

case class Function(name: Name, params: Option[Seq[Expression]], distinct: Boolean = false) extends Expression:
  def needsParenthesis = false
  def children: Seq[Expression] = params.getOrElse(Seq())
  def rebuild(c: Seq[Expression]): Expression = Function(name, params.map(_ => c), distinct)

abstract class NegatableExpression extends Expression:
  def toNegatedString: String

case class IsNull(target: Expression) extends NegatableExpression:
  def needsParenthesis = false
  def children: Seq[Expression] = Seq(target)
  def rebuild(c: Seq[Expression]): Expression = IsNull(c(0))
  def toNegatedString: String = "" // Will be implemented by renderer

case class Not(expr: Expression) extends Expression:
  def needsParenthesis = false
  def children: Seq[Expression] = Seq(expr)
  def rebuild(c: Seq[Expression]): Expression = Not(c(0))

case class Subquery(query: SelectBody) extends Expression:
  def needsParenthesis = true
  def children: Seq[Expression] = Seq()
  def rebuild(c: Seq[Expression]): Expression = this

case class WindowFunction(
  function: Function,
  partitionBy: Option[Seq[Expression]] = None,
  orderBy: Seq[OrderBy] = Seq()
) extends Expression:
  def needsParenthesis = false
  def children: Seq[Expression] =
    function.children ++ partitionBy.getOrElse(Seq()) ++ orderBy.map(_.expression)
  def rebuild(c: Seq[Expression]): Expression =
    val funcChildren = function.children
    val funcRebuilt = Function(function.name, function.params.map(_ => c.take(funcChildren.length)), function.distinct)
    val partitionRebuilt =
      partitionBy.map(_ => c.slice(funcChildren.length, funcChildren.length + partitionBy.get.length))
    val orderByRebuilt = orderBy.zipWithIndex.map { case (ob, idx) =>
      OrderBy(c(funcChildren.length + partitionBy.map(_.length).getOrElse(0) + idx), ob.ascending)
    }
    WindowFunction(funcRebuilt, partitionRebuilt, orderByRebuilt)

case class JDBCVar() extends Expression:
  def needsParenthesis = false
  def children: Seq[Expression] = Seq()
  def rebuild(c: Seq[Expression]): Expression = this

case class CaseWhenElse(target: Option[Expression], cases: Seq[(Expression, Expression)], otherwise: Expression)
    extends Expression:
  def needsParenthesis = false
  def children: Seq[Expression] =
    target.toSeq ++ Seq(otherwise) ++ cases.flatMap(x => Seq(x._1, x._2))
  def rebuild(c: Seq[Expression]): Expression =
    val (newTarget, newOtherwise, newCases) =
      if c.length % 2 == 0 then (Some(c.head), c.tail.head, c.tail.tail)
      else (None, c.head, c.tail)
    CaseWhenElse(newTarget, newCases.grouped(2).map(x => (x(0), x(1))).toSeq, newOtherwise)

case class Cast(expression: Expression, t: Name, format: Option[String] = None) extends Expression:
  def needsParenthesis = false
  def children: Seq[Expression] = Seq(expression)
  def rebuild(c: Seq[Expression]): Expression = Cast(c(0), t, format)

case class InExpression(expression: Expression, source: Either[Seq[Expression], SelectBody])
    extends NegatableExpression:
  def needsParenthesis = false
  def children: Seq[Expression] =
    Seq(expression) ++ (source match
      case Left(expr) => expr
      case Right(_) => Seq())
  def rebuild(c: Seq[Expression]): Expression =
    InExpression(
      c.head,
      source match
        case Left(_) => Left(c.tail)
        case Right(query) => Right(query)
    )
  def toNegatedString: String = "" // Will be implemented by renderer
