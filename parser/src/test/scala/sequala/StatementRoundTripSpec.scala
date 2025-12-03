package sequala

import org.scalacheck.{Arbitrary, Gen, Prop, Properties}
import sequala.schema.ast.*
import sequala.common.parser.SQL
import sequala.common.statement.*
import sequala.schema.statement.*
import sequala.schema.{Column as SchemaColumn, CreateTable as SchemaCreateTable, *}
import fastparse.Parsed

abstract class StatementRoundTripSpec(name: String) extends Properties(name) { self: DbDialect =>

  type CreateGenericTable = SchemaCreateTable[DataType, ColumnOptions, TableOptions]

  val genName: Gen[Name] = for {
    first <- Gen.alphaChar
    rest <- Gen.listOfN(5, Gen.alphaNumChar)
  } yield Name((first +: rest).mkString)

  val genLongPrimitive: Gen[LongPrimitive] =
    Gen.chooseNum(0L, 10000L).map(LongPrimitive(_))

  val genDoublePrimitive: Gen[DoublePrimitive] =
    Gen.chooseNum(0.0, 10000.0).map(DoublePrimitive(_))

  val genStringPrimitive: Gen[StringPrimitive] =
    Gen.alphaNumStr.map(s => StringPrimitive(s.take(20)))

  val genBooleanPrimitive: Gen[BooleanPrimitive] =
    Gen.oneOf(true, false).map(BooleanPrimitive(_))

  val genPrimitive: Gen[PrimitiveValue] =
    Gen.oneOf(genLongPrimitive, genDoublePrimitive, genStringPrimitive, genBooleanPrimitive)

  val genColumn: Gen[Column] = for {
    col <- genName
    table <- Gen.option(genName)
  } yield Column(col, table)

  def genArithmetic(depth: Int): Gen[Arithmetic] = for {
    lhs <- genExpression(depth - 1)
    op <- Gen.oneOf(Arithmetic.Op.Add, Arithmetic.Op.Sub, Arithmetic.Op.Mult, Arithmetic.Op.Div)
    rhs <- genExpression(depth - 1)
  } yield Arithmetic(lhs, op, rhs)

  def genComparison(depth: Int): Gen[Comparison] = for {
    lhs <- genExpression(depth - 1)
    op <- Gen.oneOf(
      Comparison.Op.Eq,
      Comparison.Op.Neq,
      Comparison.Op.Gt,
      Comparison.Op.Lt,
      Comparison.Op.Gte,
      Comparison.Op.Lte
    )
    rhs <- genExpression(depth - 1)
  } yield Comparison(lhs, op, rhs)

  val genFunction: Gen[Function] = for {
    name <- genName
    numArgs <- Gen.chooseNum(0, 3)
    args <- Gen.listOfN(numArgs, genPrimitive.map(_.asInstanceOf[Expression]))
  } yield Function(name, Some(args))

  def genExpression(depth: Int): Gen[Expression] =
    if depth <= 0 then Gen.oneOf(genPrimitive, genColumn)
    else
      Gen.frequency(
        (3, genPrimitive),
        (2, genColumn),
        (1, genArithmetic(depth)),
        (1, genComparison(depth)),
        (1, genFunction)
      )

  implicit val arbExpression: Arbitrary[Expression] = Arbitrary(genExpression(2))

  val genSelectAll: Gen[SelectAll] = Gen.const(SelectAll())

  val genSelectExpression: Gen[SelectExpression] = for {
    expr <- genExpression(1)
    alias <- Gen.option(genName)
  } yield SelectExpression(expr, alias)

  val genSelectTarget: Gen[SelectTarget] =
    Gen.frequency((1, genSelectAll), (3, genSelectExpression))

  val genFromTable: Gen[FromTable] = for {
    table <- genName
    alias <- Gen.option(genName)
  } yield FromTable(None, table, alias)

  val genOrderBy: Gen[OrderBy] = for {
    expr <- genColumn.map(_.asInstanceOf[Expression])
    asc <- Gen.oneOf(true, false)
  } yield OrderBy(expr, asc)

  val genSelectBody: Gen[SelectBody] = for {
    distinct <- Gen.oneOf(true, false)
    numTargets <- Gen.chooseNum(1, 3)
    targets <- Gen.listOfN(numTargets, genSelectTarget)
    numFroms <- Gen.chooseNum(0, 2)
    froms <- Gen.listOfN(numFroms, genFromTable)
    where <- Gen.option(genExpression(1))
    numOrderBy <- Gen.chooseNum(0, 2)
    orderBys <- Gen.listOfN(numOrderBy, genOrderBy)
    limit <- Gen.option(Gen.chooseNum(1L, 100L))
    offset <- Gen.option(Gen.chooseNum(0L, 50L))
  } yield SelectBody(
    distinct = distinct,
    target = targets,
    from = froms,
    where = where,
    groupBy = None,
    having = None,
    orderBy = orderBys,
    limit = limit,
    offset = offset,
    union = None
  )

  val genSelect: Gen[Select] = genSelectBody.map(body => Select(body))

  val genUpdate: Gen[Update] = for {
    table <- genName
    numSets <- Gen.chooseNum(1, 3)
    sets <- Gen.listOfN(
      numSets,
      for {
        col <- genName
        expr <- genExpression(1)
      } yield (col, expr)
    )
    where <- Gen.option(genExpression(1))
  } yield Update(table, sets, where)

  val genDelete: Gen[Delete] = for {
    table <- genName
    where <- Gen.option(genExpression(1))
  } yield Delete(table, where)

  val genExplicitInsert: Gen[ExplicitInsert] = for {
    numRows <- Gen.chooseNum(1, 3)
    numCols <- Gen.chooseNum(1, 3)
    rows <- Gen.listOfN(numRows, Gen.listOfN(numCols, genPrimitive.map(_.asInstanceOf[Expression])))
  } yield ExplicitInsert(rows)

  val genInsert: Gen[Insert] = for {
    table <- genName
    numCols <- Gen.chooseNum(1, 3)
    cols <- Gen.listOfN(numCols, genName)
    values <- genExplicitInsert
  } yield Insert(table, Some(cols), values, orReplace = false)

  // Abstract methods for dialect-specific generators
  def genSchemaDataType: Gen[DataType]
  def genColumnOptions: Gen[ColumnOptions]
  def genTableOptions: Gen[TableOptions]
  def genDropOptions: Gen[DropOptions]
  def genCreateViewOptions: Gen[CreateViewOptions]
  def genDropViewOptions: Gen[DropViewOptions]
  def genExplainOptions: Gen[ExplainOptions]
  def parseStatement(sql: String): Parsed[Statement]

  // Abstract method to provide renderer for statements
  def statementRenderer(using config: SqlFormatConfig): SqlRenderer[Statement]

  def genSchemaColumn: Gen[SchemaColumn[DataType, ColumnOptions]] = for {
    name <- genName
    dataType <- genSchemaDataType
    nullable <- Gen.oneOf(true, false)
    columnOptions <- genColumnOptions
  } yield SchemaColumn(name.name, dataType, nullable, None, columnOptions)

  def genCreateTableStatement: Gen[CreateGenericTable] = for {
    name <- genName
    numCols <- Gen.chooseNum(1, 3)
    cols <- Gen.listOfN(numCols, genSchemaColumn)
    tableOptions <- genTableOptions
  } yield {
    val table = Table(name.name, cols, None, Seq.empty, Seq.empty, Seq.empty, Seq.empty, tableOptions)
    SchemaCreateTable(table, orReplace = false, ifNotExists = false)
  }

  val genCreateTableAs: Gen[CreateTableAs] = for {
    name <- genName
    query <- genSelectBody
  } yield CreateTableAs(name, orReplace = false, query)

  val genCreateView: Gen[CreateView[CreateViewOptions]] = for {
    name <- genName
    query <- genSelectBody
    options <- genCreateViewOptions
  } yield CreateView(name, orReplace = false, query, options)

  val genDropTable: Gen[DropTable[DropOptions]] = for {
    name <- genName
    ifExists <- Gen.oneOf(true, false)
    options <- genDropOptions
  } yield DropTable(name.name, ifExists, options)

  val genDropView: Gen[DropView[DropViewOptions]] = for {
    name <- genName
    ifExists <- Gen.oneOf(true, false)
    options <- genDropViewOptions
  } yield DropView(name, ifExists, options)

  val genExplain: Gen[Explain[ExplainOptions]] = for {
    query <- genSelectBody
    options <- genExplainOptions
  } yield Explain(query, options)

  val genStatement: Gen[Statement] = Gen.oneOf(
    genSelect,
    genUpdate,
    genDelete,
    genInsert,
    genCreateTableStatement,
    genCreateTableAs,
    genCreateView,
    genDropTable,
    genDropView,
    genExplain
  )

  implicit val arbStatement: Arbitrary[Statement] = Arbitrary(genStatement)

  def statementsEqual(original: Statement, parsed: Statement): Boolean = (original, parsed) match {
    case (o: CreateGenericTable, p: CreateGenericTable) =>
      o.table.name == p.table.name &&
      o.table.columns.map(c => (c.name, c.nullable)) == p.table.columns.map(c => (c.name, c.nullable)) &&
      o.orReplace == p.orReplace
    case _ => original == parsed
  }

  property("serialization and parsing round-trip") = Prop.forAll(genStatement) { stmt =>
    import sequala.schema.SqlRenderer.toSql
    given SqlFormatConfig = SqlFormatConfig.Compact
    given SqlRenderer[Statement] = statementRenderer
    val sql = stmt.toSql
    parseStatement(sql) match {
      case Parsed.Success(parsedStmt, _) =>
        val equal = statementsEqual(stmt, parsedStmt)
        if !equal then {
          println(s"MISMATCH!")
          println(s"Original: $stmt")
          println(s"SQL: $sql")
          println(s"Parsed: $parsedStmt")
        }
        equal
      case f: Parsed.Failure =>
        println(s"Failed to parse: $sql")
        println(s"Error: ${f.trace().longMsg}")
        false
    }
  }
}

// Companion object for ANSI SQL round-trip tests
object StatementRoundTripSpec extends StatementRoundTripSpec("Statement round-trip") with AnsiDialect {

  override def genSchemaDataType: Gen[DataType] =
    Gen.oneOf(Gen.const(SqlInteger), Gen.const(Real), Gen.choose(1, 255).map(n => VarChar(n)))

  override def genColumnOptions: Gen[ColumnOptions] =
    Gen.const(NoColumnOptions)

  override def genTableOptions: Gen[TableOptions] =
    Gen.const(NoTableOptions)

  override def genDropOptions: Gen[DropOptions] =
    Gen.const(CommonDropOptions())

  override def genCreateViewOptions: Gen[CreateViewOptions] =
    Gen.const(CommonCreateViewOptions())

  override def genDropViewOptions: Gen[DropViewOptions] =
    Gen.const(CommonDropViewOptions())

  override def genExplainOptions: Gen[ExplainOptions] =
    Gen.const(CommonExplainOptions())

  override def parseStatement(sql: String): Parsed[Statement] =
    SQL(sql)

  override def statementRenderer(using config: SqlFormatConfig): SqlRenderer[Statement] = {
    import sequala.common.renderer.ParserSqlRenderers.given
    summon[SqlRenderer[Statement]]
  }
}
