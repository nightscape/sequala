package sequala.schema.ast

/** Schema-level AST for SELECT queries, mirroring parser AST structure */
case class SelectBody(
  distinct: Boolean = false,
  target: Seq[SelectTarget] = Seq(),
  from: Seq[FromElement] = Seq(),
  where: Option[Expression] = None,
  groupBy: Option[Seq[Expression]] = None,
  having: Option[Expression] = None,
  orderBy: Seq[OrderBy] = Seq(),
  limit: Option[Long] = None,
  offset: Option[Long] = None,
  union: Option[(Union.Type, SelectBody)] = None
):
  def unionWith(t: Union.Type, body: SelectBody): SelectBody =
    val replacementUnion =
      union match
        case Some(nested) => (nested._1, nested._2.unionWith(t, body))
        case None => (t, body)

    SelectBody(distinct, target, from, where, groupBy, having, orderBy, limit, offset, union = Some(replacementUnion))

object Union:
  enum Type:
    case All, Distinct

  def toSql(t: Type): String = t match
    case Type.All => "UNION ALL"
    case Type.Distinct => "UNION DISTINCT"
