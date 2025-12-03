package sequala.common.renderer

import sequala.schema.ast.*
import sequala.common.statement.*
import sequala.schema.statement.*
import sequala.schema.{
  AlterTable,
  AlterTableAction,
  CreateViewOptions,
  DbDialect,
  DropOptions,
  EmptyStatement,
  IdentifierQuoting,
  SqlFormatConfig,
  SqlRenderer,
  Statement,
  Unparseable
}

abstract class ParserSqlRenderers:
  self: DbDialect =>

  given dropOptionsRenderer: SqlRenderer[DropOptions]
  given alterTableActionRenderer: SqlRenderer[AlterTableAction[DataType, ColumnOptions]]

  def parenthesize(e: Expression)(using config: SqlFormatConfig): String =
    val renderer = summon[SqlRenderer[Expression]]
    if e.needsParenthesis then s"(${renderer.toSql(e)})" else renderer.toSql(e)

  given SqlRenderer[Name] with
    def toSql(name: Name)(using config: SqlFormatConfig): String =
      if name.quoted then s""""${name.name.replace("\"", "\"\"")}"""" else name.name

  given SqlRenderer[PrimitiveValue] with
    def toSql(pv: PrimitiveValue)(using config: SqlFormatConfig): String = pv match
      case LongPrimitive(v) => v.toString
      case DoublePrimitive(v) => v.toString
      case StringPrimitive(v) => s"'${Expression.escapeString(v)}'"
      case BooleanPrimitive(v) => v.toString
      case NullPrimitive() => "NULL"

  given (using nameRenderer: SqlRenderer[Name]): SqlRenderer[Identifier] with
    def toSql(id: Identifier)(using config: SqlFormatConfig): String =
      nameRenderer.toSql(id.name)

  given (using nameRenderer: SqlRenderer[Name]): SqlRenderer[Column] with
    def toSql(col: Column)(using config: SqlFormatConfig): String =
      (col.table.toSeq ++ Seq(col.column)).map(nameRenderer.toSql).mkString(".")

  given SqlRenderer[Arithmetic] with
    def toSql(arith: Arithmetic)(using config: SqlFormatConfig): String =
      parenthesize(arith.lhs) + " " + Arithmetic.opString(arith.op) + " " + parenthesize(arith.rhs)

  given SqlRenderer[Comparison] with
    def toSql(comp: Comparison)(using config: SqlFormatConfig): String =
      parenthesize(comp.lhs) + " " + Comparison.opString(comp.op) + " " + parenthesize(comp.rhs)

  given (using nameRenderer: SqlRenderer[Name], exprRenderer: SqlRenderer[Expression]): SqlRenderer[Function] with
    def toSql(func: Function)(using config: SqlFormatConfig): String =
      val distinctStr = if func.distinct then "DISTINCT " else ""
      val paramsStr = func.params.map(_.map(exprRenderer.toSql).mkString(", ")).getOrElse("*")
      s"${nameRenderer.toSql(func.name)}($distinctStr$paramsStr)"

  given (using
    funcRenderer: SqlRenderer[Function],
    exprRenderer: SqlRenderer[Expression],
    orderByRenderer: SqlRenderer[OrderBy]
  ): SqlRenderer[WindowFunction] with
    def toSql(wf: WindowFunction)(using config: SqlFormatConfig): String =
      val funcStr = funcRenderer.toSql(wf.function)
      val overClause = if wf.partitionBy.isDefined || wf.orderBy.nonEmpty then
        val partitionStr =
          wf.partitionBy.map(exprs => "PARTITION BY " + exprs.map(exprRenderer.toSql).mkString(", ")).getOrElse("")
        val orderStr =
          if wf.orderBy.nonEmpty then "ORDER BY " + wf.orderBy.map(orderByRenderer.toSql).mkString(", ") else ""
        val parts = Seq(partitionStr, orderStr).filter(_.nonEmpty)
        s"OVER (${parts.mkString(", ")})"
      else "OVER ()"
      s"$funcStr $overClause"

  given SqlRenderer[JDBCVar] with
    def toSql(jdbcVar: JDBCVar)(using config: SqlFormatConfig): String = "?"

  given SqlRenderer[Subquery] with
    def toSql(subq: Subquery)(using config: SqlFormatConfig): String =
      val queryRenderer = summon[SqlRenderer[SelectBody]]
      s"(${queryRenderer.toSql(subq.query)})"

  given (using exprRenderer: SqlRenderer[Expression]): SqlRenderer[CaseWhenElse] with
    def toSql(cwe: CaseWhenElse)(using config: SqlFormatConfig): String =
      val targetStr = cwe.target.map(exprRenderer.toSql).getOrElse("")
      val casesStr = cwe.cases
        .map { case (when, then_) =>
          s"WHEN ${parenthesize(when)} THEN ${parenthesize(then_)}"
        }
        .mkString(" ")
      val elseStr = exprRenderer.toSql(cwe.otherwise)
      s"CASE $targetStr$casesStr ELSE $elseStr END"

  given SqlRenderer[IsNull] with
    def toSql(isNull: IsNull)(using config: SqlFormatConfig): String =
      s"${parenthesize(isNull.target)} IS NULL"

  given (using exprRenderer: SqlRenderer[Expression]): SqlRenderer[InExpression] with
    def toSql(inExpr: InExpression)(using config: SqlFormatConfig): String =
      val exprStr = parenthesize(inExpr.expression)
      val sourceStr = inExpr.source match
        case Left(elems) => s"(${elems.map(e => parenthesize(e)).mkString(", ")})"
        case Right(query) =>
          val queryRenderer = summon[SqlRenderer[SelectBody]]
          s"(${queryRenderer.toSql(query)})"
      s"$exprStr IN $sourceStr"

  given (using exprRenderer: SqlRenderer[Expression]): SqlRenderer[Not] with
    def toSql(not: Not)(using config: SqlFormatConfig): String =
      not.expr match
        case isNull: IsNull =>
          s"${parenthesize(isNull.target)} IS NOT NULL"
        case inExpr: InExpression =>
          val exprStr = parenthesize(inExpr.expression)
          val sourceStr = inExpr.source match
            case Left(elems) => s"(${elems.map(e => parenthesize(e)).mkString(", ")})"
            case Right(query) =>
              val queryRenderer = summon[SqlRenderer[SelectBody]]
              s"(${queryRenderer.toSql(query)})"
          s"$exprStr NOT IN $sourceStr"
        case _ =>
          s"NOT ${parenthesize(not.expr)}"

  given (using exprRenderer: SqlRenderer[Expression], nameRenderer: SqlRenderer[Name]): SqlRenderer[Cast] with
    def toSql(cast: Cast)(using config: SqlFormatConfig): String =
      val formatStr = cast.format.map(f => s", '$f'").getOrElse("")
      s"CAST(${exprRenderer.toSql(cast.expression)} AS ${nameRenderer.toSql(cast.t)}$formatStr)"

  given SqlRenderer[Expression] with
    def toSql(expr: Expression)(using config: SqlFormatConfig): String = expr match
      case pv: PrimitiveValue => summon[SqlRenderer[PrimitiveValue]].toSql(pv)
      case id: Identifier => summon[SqlRenderer[Identifier]].toSql(id)
      case col: Column => summon[SqlRenderer[Column]].toSql(col)
      case arith: Arithmetic => summon[SqlRenderer[Arithmetic]].toSql(arith)
      case comp: Comparison => summon[SqlRenderer[Comparison]].toSql(comp)
      case func: Function => summon[SqlRenderer[Function]].toSql(func)
      case wf: WindowFunction => summon[SqlRenderer[WindowFunction]].toSql(wf)
      case jdbcVar: JDBCVar => summon[SqlRenderer[JDBCVar]].toSql(jdbcVar)
      case subq: Subquery => summon[SqlRenderer[Subquery]].toSql(subq)
      case cwe: CaseWhenElse => summon[SqlRenderer[CaseWhenElse]].toSql(cwe)
      case isNull: IsNull => summon[SqlRenderer[IsNull]].toSql(isNull)
      case not: Not => summon[SqlRenderer[Not]].toSql(not)
      case cast: Cast => summon[SqlRenderer[Cast]].toSql(cast)
      case inExpr: InExpression => summon[SqlRenderer[InExpression]].toSql(inExpr)
      case isNull: IsNull => summon[SqlRenderer[IsNull]].toSql(isNull)
      case not: Not => summon[SqlRenderer[Not]].toSql(not)

  given (using nameRenderer: SqlRenderer[Name], exprRenderer: SqlRenderer[Expression]): SqlRenderer[SelectTarget] with
    def toSql(st: SelectTarget)(using config: SqlFormatConfig): String = st match
      case SelectAll() => "*"
      case SelectTable(table) =>
        s"${nameRenderer.toSql(table)}.*"
      case SelectExpression(expression, alias) =>
        val exprStr = exprRenderer.toSql(expression)
        val aliasStr = alias.map(a => s" AS ${nameRenderer.toSql(a)}").getOrElse("")
        s"$exprStr$aliasStr"

  given (using exprRenderer: SqlRenderer[Expression]): SqlRenderer[OrderBy] with
    def toSql(ob: OrderBy)(using config: SqlFormatConfig): String =
      val exprStr = exprRenderer.toSql(ob.expression)
      val descStr = if ob.ascending then "" else " DESC"
      s"$exprStr$descStr"

  given (using nameRenderer: SqlRenderer[Name], exprRenderer: SqlRenderer[Expression]): SqlRenderer[FromElement] with
    def toSql(fe: FromElement)(using config: SqlFormatConfig): String = fe match
      case FromTable(schema, table, alias) =>
        val schemaStr = schema.map(s => s"${nameRenderer.toSql(s)}.").getOrElse("")
        val aliasStr = alias.map(a => s" AS ${nameRenderer.toSql(a)}").getOrElse("")
        s"$schemaStr${nameRenderer.toSql(table)}$aliasStr"
      case FromSelect(body, alias) =>
        val queryRenderer = summon[SqlRenderer[SelectBody]]
        s"(${queryRenderer.toSql(body)}) AS ${nameRenderer.toSql(alias)}"
      case FromJoin(lhs, rhs, joinType, on, alias) =>
        val fromRenderer = summon[SqlRenderer[FromElement]]
        def toSqlWithParensIfNeeded(fe: FromElement): String =
          fe match
            case _: FromJoin => s"(${fromRenderer.toSql(fe)})"
            case _ => fromRenderer.toSql(fe)
        val lhsStr = toSqlWithParensIfNeeded(lhs)
        val rhsStr = toSqlWithParensIfNeeded(rhs)
        val joinStr = Join.toSql(joinType)
        val onStr = on match
          case BooleanPrimitive(true) => ""
          case _ => s" ON ${exprRenderer.toSql(on)}"
        val baseStr = s"$lhsStr $joinStr $rhsStr$onStr"
        alias match
          case Some(a) => s"($baseStr) AS ${nameRenderer.toSql(a)}"
          case None => baseStr

  given (using
    targetRenderer: SqlRenderer[SelectTarget],
    fromRenderer: SqlRenderer[FromElement],
    exprRenderer: SqlRenderer[Expression],
    orderByRenderer: SqlRenderer[OrderBy]
  ): SqlRenderer[SelectBody] with
    def toSql(sb: SelectBody)(using config: SqlFormatConfig): String =
      val distinctStr = if sb.distinct then "DISTINCT " else ""
      val targetStr = sb.target.map(targetRenderer.toSql).mkString(", ")
      val fromStr = if sb.from.isEmpty then "" else s"FROM ${sb.from.map(fromRenderer.toSql).mkString(", ")}"
      val whereStr = sb.where.map(w => s"WHERE ${exprRenderer.toSql(w)}").getOrElse("")
      val groupByStr = sb.groupBy.map(gb => s"GROUP BY ${gb.map(exprRenderer.toSql).mkString(", ")}").getOrElse("")
      val havingStr = sb.having.map(h => s"HAVING ${exprRenderer.toSql(h)}").getOrElse("")
      val orderByStr =
        if sb.orderBy.isEmpty then "" else s"ORDER BY ${sb.orderBy.map(orderByRenderer.toSql).mkString(", ")}"
      val limitStr = sb.limit.map(l => s"LIMIT $l").getOrElse("")
      val offsetStr = sb.offset.map(o => s"OFFSET $o").getOrElse("")
      val unionStr = sb.union
        .map { case (t, b) =>
          val queryRenderer = summon[SqlRenderer[SelectBody]]
          s"${Union.toSql(t)} ${queryRenderer.toSql(b)}"
        }
        .getOrElse("")

      Seq(
        "SELECT",
        distinctStr + targetStr,
        fromStr,
        whereStr,
        groupByStr,
        havingStr,
        orderByStr,
        limitStr,
        offsetStr,
        unionStr
      )
        .filter(_.nonEmpty)
        .mkString(" ")

  // Parser AST renderers (still needed for parser internals)
  given (using nameRenderer: SqlRenderer[Name], queryRenderer: SqlRenderer[SelectBody]): SqlRenderer[WithClause] with
    def toSql(wc: WithClause)(using config: SqlFormatConfig): String =
      s"${nameRenderer.toSql(wc.name)} AS (${queryRenderer.toSql(wc.body)})"

  given (using exprRenderer: SqlRenderer[Expression]): SqlRenderer[ColumnAnnotation] with
    def toSql(ca: ColumnAnnotation)(using config: SqlFormatConfig): String = ca match
      case ColumnIsPrimaryKey() => "PRIMARY KEY"
      case ColumnIsNotNullable() => "NOT NULL"
      case ColumnIsNullable() => "NULL"
      case ColumnDefaultValue(v) =>
        s"DEFAULT ${exprRenderer.toSql(v)}"

  given (using
    nameRenderer: SqlRenderer[Name],
    primRenderer: SqlRenderer[PrimitiveValue],
    annRenderer: SqlRenderer[ColumnAnnotation]
  ): SqlRenderer[ColumnDefinition] with
    def toSql(cd: ColumnDefinition)(using config: SqlFormatConfig): String =
      val nameStr = nameRenderer.toSql(cd.name)
      val typeStr = nameRenderer.toSql(cd.t)
      val argsStr = if cd.args.isEmpty then "" else s"(${cd.args.map(primRenderer.toSql).mkString(", ")})"
      val annsStr = cd.annotations.map(annRenderer.toSql).mkString(" ")
      s"$nameStr $typeStr$argsStr $annsStr".trim

  given (using nameRenderer: SqlRenderer[Name], exprRenderer: SqlRenderer[Expression]): SqlRenderer[TableAnnotation]
  with
    def toSql(ta: TableAnnotation)(using config: SqlFormatConfig): String = ta match
      case TablePrimaryKey(columns, name) =>
        val constraintName = name.map(n => s"CONSTRAINT ${nameRenderer.toSql(n)} ").getOrElse("")
        s"${constraintName}PRIMARY KEY (${columns.map(nameRenderer.toSql).mkString(", ")})"
      case TableIndexOn(columns) =>
        s"INDEX (${columns.map(nameRenderer.toSql).mkString(", ")})"
      case TableUnique(columns, name) =>
        val constraintName = name.map(n => s"CONSTRAINT ${nameRenderer.toSql(n)} ").getOrElse("")
        s"${constraintName}UNIQUE (${columns.map(nameRenderer.toSql).mkString(", ")})"
      case TableForeignKey(name, columns, refTable, refColumns, onUpdate, onDelete) =>
        val constraintName = name.map(n => s"CONSTRAINT ${nameRenderer.toSql(n)} ").getOrElse("")
        val colList = columns.map(nameRenderer.toSql).mkString(", ")
        val refColList = refColumns.map(nameRenderer.toSql).mkString(", ")
        val onUpdateStr = onUpdate.map(a => s" ON UPDATE ${a.toSql}").getOrElse("")
        val onDeleteStr = onDelete.map(a => s" ON DELETE ${a.toSql}").getOrElse("")
        s"${constraintName}FOREIGN KEY ($colList) REFERENCES ${nameRenderer.toSql(refTable)}($refColList)$onUpdateStr$onDeleteStr"
      case TableCheck(name, expression) =>
        val constraintName = name.map(n => s"CONSTRAINT ${nameRenderer.toSql(n)} ").getOrElse("")
        s"${constraintName}CHECK (${exprRenderer.toSql(expression)})"

  given SqlRenderer[AlterViewAction] with
    def toSql(ava: AlterViewAction)(using config: SqlFormatConfig): String = ava match
      case Materialize(add) =>
        if add then "MATERIALIZE" else "DROP MATERIALIZE"

  given (using
    exprRenderer: SqlRenderer[sequala.schema.ast.Expression],
    queryRenderer: SqlRenderer[sequala.schema.ast.SelectBody]
  ): SqlRenderer[sequala.schema.ast.InsertValues] with
    def toSql(iv: sequala.schema.ast.InsertValues)(using config: SqlFormatConfig): String = iv match
      case sequala.schema.ast.ExplicitInsert(values) =>
        val rows = values.map(row => s"(${row.map(exprRenderer.toSql).mkString(", ")})")
        s"VALUES ${rows.mkString(", ")}"
      case sequala.schema.ast.SelectInsert(query) =>
        queryRenderer.toSql(query)

  given SqlRenderer[EmptyStatement] with
    def toSql(es: EmptyStatement)(using config: SqlFormatConfig): String = ""

  given SqlRenderer[Unparseable] with
    def toSql(u: Unparseable)(using config: SqlFormatConfig): String = u.content

  // Schema-level statement renderers using AST types
  given (using
    nameRenderer: SqlRenderer[sequala.schema.ast.Name],
    queryRenderer: SqlRenderer[sequala.schema.ast.SelectBody],
    withRenderer: SqlRenderer[sequala.schema.ast.WithClause]
  ): SqlRenderer[sequala.schema.Select] with
    def toSql(s: sequala.schema.Select)(using config: SqlFormatConfig): String =
      val withStr =
        if s.withClause.isEmpty then ""
        else s"WITH ${s.withClause.map(withRenderer.toSql).mkString(", ")}\n"
      s"$withStr${queryRenderer.toSql(s.body)};"

  given (using
    nameRenderer: SqlRenderer[sequala.schema.ast.Name],
    exprRenderer: SqlRenderer[sequala.schema.ast.Expression]
  ): SqlRenderer[sequala.schema.Update] with
    def toSql(u: sequala.schema.Update)(using config: SqlFormatConfig): String =
      val setStr =
        u.set.map { case (name, expr) => s"${nameRenderer.toSql(name)} = ${exprRenderer.toSql(expr)}" }.mkString(", ")
      val whereStr = u.where.map(w => s" WHERE ${exprRenderer.toSql(w)}").getOrElse("")
      s"UPDATE ${nameRenderer.toSql(u.table)} SET $setStr$whereStr;"

  given (using
    nameRenderer: SqlRenderer[sequala.schema.ast.Name],
    exprRenderer: SqlRenderer[sequala.schema.ast.Expression]
  ): SqlRenderer[sequala.schema.Delete] with
    def toSql(d: sequala.schema.Delete)(using config: SqlFormatConfig): String =
      val whereStr = d.where.map(w => s" WHERE ${exprRenderer.toSql(w)}").getOrElse("")
      s"DELETE FROM ${nameRenderer.toSql(d.table)}$whereStr;"

  given (using
    nameRenderer: SqlRenderer[sequala.schema.ast.Name],
    valuesRenderer: SqlRenderer[sequala.schema.ast.InsertValues]
  ): SqlRenderer[sequala.schema.Insert] with
    def toSql(i: sequala.schema.Insert)(using config: SqlFormatConfig): String =
      val orReplaceStr = if i.orReplace then " OR REPLACE " else ""
      val columnsStr = i.columns.map(cols => s"(${cols.map(nameRenderer.toSql).mkString(", ")})").getOrElse("")
      s"INSERT$orReplaceStr INTO ${nameRenderer.toSql(i.table)}$columnsStr ${valuesRenderer.toSql(i.values)};"

  given (using
    nameRenderer: SqlRenderer[sequala.schema.ast.Name],
    exprRenderer: SqlRenderer[sequala.schema.ast.Expression],
    queryRenderer: SqlRenderer[sequala.schema.ast.SelectBody]
  ): SqlRenderer[sequala.schema.Merge] with
    def toSql(m: sequala.schema.Merge)(using config: SqlFormatConfig): String =
      val intoStr = nameRenderer.toSql(m.intoTable)
      val intoAliasStr = m.intoAlias.map(a => s" $a").getOrElse("")

      val usingStr = m.usingSource match
        case sequala.schema.MergeSourceTable(table, alias) =>
          val aliasStr = alias.map(a => s" $a").getOrElse("")
          s"${nameRenderer.toSql(table)}$aliasStr"
        case sequala.schema.MergeSourceSelect(query, alias) =>
          s"(${queryRenderer.toSql(query)}) $alias"

      val onStr = exprRenderer.toSql(m.onCondition)

      val matchedStr = m.whenMatched
        .map { wm =>
          val setStr =
            wm.set.map { case (n, e) => s"${nameRenderer.toSql(n)} = ${exprRenderer.toSql(e)}" }.mkString(", ")
          val whereStr = wm.where.map(w => s" WHERE ${exprRenderer.toSql(w)}").getOrElse("")
          s" WHEN MATCHED THEN UPDATE SET $setStr$whereStr"
        }
        .getOrElse("")

      val notMatchedStr = m.whenNotMatched
        .map { wnm =>
          val colStr = wnm.columns.map(nameRenderer.toSql).mkString(", ")
          val valStr = wnm.values.map(exprRenderer.toSql).mkString(", ")
          s" WHEN NOT MATCHED THEN INSERT ($colStr) VALUES ($valStr)"
        }
        .getOrElse("")

      s"MERGE INTO $intoStr$intoAliasStr USING $usingStr ON ($onStr)$matchedStr$notMatchedStr;"

  given (using
    nameRenderer: SqlRenderer[sequala.schema.ast.Name],
    queryRenderer: SqlRenderer[sequala.schema.ast.SelectBody]
  ): SqlRenderer[sequala.schema.CreateTableAs] with
    def toSql(cta: sequala.schema.CreateTableAs)(using config: SqlFormatConfig): String =
      val orReplaceStr = if cta.orReplace then "OR REPLACE " else ""
      s"CREATE $orReplaceStr TABLE ${nameRenderer.toSql(cta.name)} AS ${queryRenderer.toSql(cta.query)};"

  given (using
    nameRenderer: SqlRenderer[sequala.schema.ast.Name],
    queryRenderer: SqlRenderer[sequala.schema.ast.SelectBody]
  ): SqlRenderer[sequala.schema.CreateView[CreateViewOptions]] with
    def toSql(cv: sequala.schema.CreateView[CreateViewOptions])(using config: SqlFormatConfig): String =
      val orReplaceStr = if cv.orReplace then "OR REPLACE " else ""
      s"CREATE $orReplaceStr VIEW ${nameRenderer.toSql(cv.name)} AS ${queryRenderer.toSql(cv.query)};"

  given (using
    nameRenderer: SqlRenderer[sequala.schema.ast.Name],
    actionRenderer: SqlRenderer[sequala.schema.ast.AlterViewAction]
  ): SqlRenderer[sequala.schema.AlterView] with
    def toSql(av: sequala.schema.AlterView)(using config: SqlFormatConfig): String =
      s"ALTER VIEW ${nameRenderer.toSql(av.name)} ${actionRenderer.toSql(av.action)};"

  given (using nameRenderer: SqlRenderer[sequala.schema.ast.Name]): SqlRenderer[
    sequala.schema.DropView[DropViewOptions]
  ] with
    def toSql(dv: sequala.schema.DropView[DropViewOptions])(using config: SqlFormatConfig): String =
      val ifExistsStr = if dv.ifExists then "IF EXISTS " else ""
      s"DROP VIEW $ifExistsStr${nameRenderer.toSql(dv.name)};"

  given (using queryRenderer: SqlRenderer[sequala.schema.ast.SelectBody]): SqlRenderer[
    sequala.schema.Explain[ExplainOptions]
  ] with
    def toSql(e: sequala.schema.Explain[ExplainOptions])(using config: SqlFormatConfig): String =
      s"EXPLAIN ${queryRenderer.toSql(e.query)};"

  given SqlRenderer[sequala.schema.CreateIndex[IndexOptions]] with
    def toSql(ci: sequala.schema.CreateIndex[IndexOptions])(using config: SqlFormatConfig): String =
      val uniqueStr = if ci.unique then "UNIQUE " else ""
      val ifNotExistsStr = if ci.ifNotExists then "IF NOT EXISTS " else ""
      val colsStr = ci.columns.map(col => col.name + (if col.descending then " DESC" else "")).mkString(", ")
      val whereStr = ci.options.where.map(w => s" WHERE $w").getOrElse("")
      s"CREATE ${uniqueStr}INDEX $ifNotExistsStr${ci.name} ON ${ci.tableName}($colsStr)$whereStr;"

  given SqlRenderer[sequala.schema.CreateGenericTable] with
    def toSql(ct: sequala.schema.CreateGenericTable)(using config: SqlFormatConfig): String =
      import sequala.schema.GenericSqlRenderer.given
      summon[SqlRenderer[sequala.schema.CreateTable[
        sequala.schema.CommonDataType,
        sequala.schema.NoColumnOptions.type,
        sequala.schema.NoTableOptions.type
      ]]].toSql(ct) + ";"

  def renderStatement(stmt: sequala.schema.Statement)(using config: SqlFormatConfig): String =
    stmt match
      case es: EmptyStatement => summon[SqlRenderer[EmptyStatement]].toSql(es)
      case u: Unparseable => summon[SqlRenderer[Unparseable]].toSql(u)
      case s: sequala.schema.Select => summon[SqlRenderer[sequala.schema.Select]].toSql(s)
      case u: sequala.schema.Update => summon[SqlRenderer[sequala.schema.Update]].toSql(u)
      case d: sequala.schema.Delete => summon[SqlRenderer[sequala.schema.Delete]].toSql(d)
      case i: sequala.schema.Insert => summon[SqlRenderer[sequala.schema.Insert]].toSql(i)
      case m: sequala.schema.Merge => summon[SqlRenderer[sequala.schema.Merge]].toSql(m)
      case cta: sequala.schema.CreateTableAs => summon[SqlRenderer[sequala.schema.CreateTableAs]].toSql(cta)
      case cv: sequala.schema.CreateView[CreateViewOptions @unchecked] =>
        summon[SqlRenderer[sequala.schema.CreateView[CreateViewOptions]]].toSql(cv)
      case av: sequala.schema.AlterView => summon[SqlRenderer[sequala.schema.AlterView]].toSql(av)
      case dt: sequala.schema.DropTable[DropOptions @unchecked] =>
        import sequala.schema.GenericSqlRenderer.given
        summon[sequala.schema.SqlRenderer[sequala.schema.DropTable[DropOptions]]].toSql(dt) + ";"
      case dv: sequala.schema.DropView[DropViewOptions @unchecked] =>
        summon[SqlRenderer[sequala.schema.DropView[DropViewOptions]]].toSql(dv)
      case e: sequala.schema.Explain[ExplainOptions @unchecked] =>
        summon[SqlRenderer[sequala.schema.Explain[ExplainOptions]]].toSql(e)
      case ci: sequala.schema.CreateIndex[IndexOptions @unchecked] =>
        summon[SqlRenderer[sequala.schema.CreateIndex[IndexOptions]]].toSql(ci)
      case ct: sequala.schema.CreateGenericTable => summon[SqlRenderer[sequala.schema.CreateGenericTable]].toSql(ct)
      case at: AlterTable[
            DataType @unchecked,
            ColumnOptions @unchecked,
            ?,
            AlterTableAction[DataType, ColumnOptions] @unchecked
          ] =>
        val actions = at.actions.map(a => alterTableActionRenderer.toSql(a)).mkString(", ")
        val quotedTable = at.tableName.split('.').map(IdentifierQuoting.DoubleQuote.quoteIdentifier).mkString(".")
        s"ALTER TABLE $quotedTable $actions;"
      case _ => throw new MatchError(s"Unsupported statement type: ${stmt.getClass}")

  given SqlRenderer[sequala.schema.Statement] with
    def toSql(stmt: sequala.schema.Statement)(using config: SqlFormatConfig): String =
      renderStatement(stmt)

object ParserSqlRenderers extends ParserSqlRenderers, sequala.schema.AnsiDialect:

  private def quote(name: String): String = IdentifierQuoting.DoubleQuote.quoteIdentifier(name)

  given dropOptionsRenderer: SqlRenderer[DropOptions] =
    sequala.schema.GenericSqlRenderer.given_SqlRenderer_CommonDropOptions

  given alterTableActionRenderer: SqlRenderer[AlterTableAction[DataType, ColumnOptions]] with
    def toSql(action: AlterTableAction[DataType, ColumnOptions])(using config: SqlFormatConfig): String =
      import sequala.schema.*
      import sequala.schema.GenericSqlRenderer.given_SqlRenderer_TableConstraint
      val leadingComments = config.renderLeadingComments(action.sourceComment)
      val sql = action match
        case DropColumn(name, cascade, _) =>
          val cascadeStr = if cascade then " CASCADE" else ""
          s"DROP COLUMN ${quote(name)}$cascadeStr"
        case RenameColumn(oldName, newName, _) =>
          s"RENAME COLUMN ${quote(oldName)} TO ${quote(newName)}"
        case DropConstraint(name, cascade, _) =>
          val cascadeStr = if cascade then " CASCADE" else ""
          s"DROP CONSTRAINT ${quote(name)}$cascadeStr"
        case AddConstraint(constraint, _) =>
          s"ADD ${summon[SqlRenderer[TableConstraint]].toSql(constraint)}"
        case _ => throw MatchError(s"Unsupported AlterTableAction: ${action.getClass}")
      s"$leadingComments$sql"
