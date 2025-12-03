package sequala.schema.ast

import sequala.schema.ast.{Expression, SelectBody}

sealed trait FromElement:
  def aliases: Seq[Name]
  def withAlias(newAlias: Name): FromElement

case class FromTable(schema: Option[Name], table: Name, alias: Option[Name] = None) extends FromElement:
  def aliases = Seq(alias.getOrElse(table))
  def withAlias(newAlias: Name) = FromTable(schema, table, Some(newAlias))

case class FromSelect(body: SelectBody, alias: Name) extends FromElement:
  def aliases = Seq(alias)
  def withAlias(newAlias: Name) = FromSelect(body, newAlias)

case class FromJoin(
  lhs: FromElement,
  rhs: FromElement,
  t: Join.Type = Join.Type.Inner,
  on: Expression,
  alias: Option[Name] = None
) extends FromElement:
  def aliases =
    alias.map(Seq(_)).getOrElse(lhs.aliases ++ rhs.aliases)
  def withAlias(newAlias: Name) = FromJoin(lhs, rhs, t, on, Some(newAlias))

object Join:
  enum Type:
    case Inner, Natural, LeftOuter, RightOuter, FullOuter

  def toSql(t: Type): String = t match
    case Type.Inner => "JOIN"
    case Type.Natural => "NATURAL JOIN"
    case Type.LeftOuter => "LEFT OUTER JOIN"
    case Type.RightOuter => "RIGHT OUTER JOIN"
    case Type.FullOuter => "FULL OUTER JOIN"
