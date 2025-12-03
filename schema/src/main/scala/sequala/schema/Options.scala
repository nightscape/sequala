package sequala.schema

trait TableOptions
trait ColumnOptions
trait DropOptions
trait CreateViewOptions
trait DropViewOptions
trait IndexOptions:
  def where: Option[String]
trait ExplainOptions
trait DiffOptions:
  def caseInsensitive: Boolean
  def defaultSchema: Option[String]
  def normalizeDataType(dt: DataType): DataType = dt
  def normalizePrimaryKey(pk: PrimaryKey): PrimaryKey = pk

case object NoTableOptions extends TableOptions
case object NoColumnOptions extends ColumnOptions
case object NoDropOptions extends DropOptions
case object NoCreateViewOptions extends CreateViewOptions
case object NoDropViewOptions extends DropViewOptions
case object NoIndexOptions extends IndexOptions:
  def where: Option[String] = None
case object NoExplainOptions extends ExplainOptions

case class CommonDropOptions(cascade: Boolean = false) extends DropOptions

object CommonDropOptions:
  val empty: CommonDropOptions = CommonDropOptions()

case class CommonCreateViewOptions(materialized: Boolean = false, temporary: Boolean = false) extends CreateViewOptions

object CommonCreateViewOptions:
  val empty: CommonCreateViewOptions = CommonCreateViewOptions()

case class CommonDropViewOptions(cascade: Boolean = false) extends DropViewOptions

object CommonDropViewOptions:
  val empty: CommonDropViewOptions = CommonDropViewOptions()

case class CommonIndexOptions(where: Option[String] = None) extends IndexOptions

object CommonIndexOptions:
  val empty: CommonIndexOptions = CommonIndexOptions()

case class CommonExplainOptions() extends ExplainOptions

object CommonExplainOptions:
  val empty: CommonExplainOptions = CommonExplainOptions()

case class CommonDiffOptions(caseInsensitive: Boolean = false, defaultSchema: Option[String] = None) extends DiffOptions

object CommonDiffOptions:
  val empty: CommonDiffOptions = CommonDiffOptions()
