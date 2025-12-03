package sequala.migrate

trait Diffable[K, V, Op]:
  def extractKey(v: V): K
  def createOp(v: V): Seq[Op]
  def deleteOp(v: V): Seq[Op]
  def modifyOp(from: V, to: V): Seq[Op] // empty if identical

object Differ:

  def diff[K, V, Op](from: Seq[V], to: Seq[V])(using d: Diffable[K, V, Op]): Seq[Op] =
    val fromByKey = from.map(v => d.extractKey(v) -> v).toMap
    val toByKey = to.map(v => d.extractKey(v) -> v).toMap

    val deletes = from.filterNot(v => toByKey.contains(d.extractKey(v))).flatMap(d.deleteOp)
    val creates = to.filterNot(v => fromByKey.contains(d.extractKey(v))).flatMap(d.createOp)
    val modifies = to.flatMap { v =>
      fromByKey.get(d.extractKey(v)).toSeq.flatMap(fromV => d.modifyOp(fromV, v))
    }

    deletes ++ creates ++ modifies
