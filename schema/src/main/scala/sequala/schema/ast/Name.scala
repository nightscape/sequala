package sequala.schema.ast

/** Name with quoting information, used throughout the AST */
case class Name(name: String, quoted: Boolean = false):
  override def equals(other: Any): Boolean =
    other match
      case s: String => equals(s)
      case n: Name => equals(n)
      case _ => false

  def equals(other: Name): Boolean =
    if quoted || other.quoted then name.equals(other.name)
    else name.equalsIgnoreCase(other.name)

  def equals(other: String): Boolean =
    if quoted then name.equals(other)
    else name.equalsIgnoreCase(other)

  def +(other: Name): Name = Name(name + other.name, quoted || other.quoted)
  def +(other: String): Name = Name(name + other, quoted)

  def lower: String = if quoted then name else name.toLowerCase
  def upper: String = if quoted then name else name.toUpperCase

  def extendUpper(template: String): Name =
    Name(template.replace("\\$", upper), quoted)

  def extendLower(template: String): Name =
    Name(template.replace("\\$", lower), quoted)

  def withPrefix(other: String): Name = Name(other + name, quoted)
  def withSuffix(other: String): Name = Name(name + other, quoted)

class StringNameMatch(cmp: String):
  def unapply(name: Name): Option[Unit] =
    if name.equals(cmp) then Some(()) else None

object NameMatch:
  def apply(cmp: String): StringNameMatch = new StringNameMatch(cmp)
