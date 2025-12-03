package sequala.schema

trait BaseSqlRenderers:
  protected val quoting: IdentifierQuoting = IdentifierQuoting.DoubleQuote
  protected def quote(name: String): String = quoting.quoteIdentifier(name)
  protected def quoteQualifiedName(name: String): String =
    name.split('.').map(quote).mkString(".")

  // Non-parameterized renderers (don't depend on CommonDataType)

  given SqlRenderer[ReferentialAction] with
    def toSql(action: ReferentialAction)(using config: SqlFormatConfig): String = action match
      case NoAction => "NO ACTION"
      case Restrict => "RESTRICT"
      case Cascade => "CASCADE"
      case SetNull => "SET NULL"
      case SetDefault => "SET DEFAULT"

  given SqlRenderer[IndexColumn] with
    def toSql(col: IndexColumn)(using config: SqlFormatConfig): String =
      val desc = if col.descending then " DESC" else ""
      val nulls = col.nullsFirst match
        case Some(true) => " NULLS FIRST"
        case Some(false) => " NULLS LAST"
        case None => ""
      s"${quote(col.name)}$desc$nulls"

  given SqlRenderer[PrimaryKey] with
    def toSql(pk: PrimaryKey)(using config: SqlFormatConfig): String =
      val nameStr = pk.name.map(n => s"CONSTRAINT ${quote(n)} ").getOrElse("")
      val cols = pk.columns.map(quote).mkString(", ")
      s"${nameStr}PRIMARY KEY ($cols)"

  given SqlRenderer[ForeignKey] with
    def toSql(fk: ForeignKey)(using config: SqlFormatConfig): String =
      val nameStr = fk.name.map(n => s"CONSTRAINT ${quote(n)} ").getOrElse("")
      val cols = fk.columns.map(quote).mkString(", ")
      val refCols = fk.refColumns.map(quote).mkString(", ")
      def renderAction(action: ReferentialAction): String = action match
        case NoAction => "NO ACTION"
        case Restrict => "RESTRICT"
        case Cascade => "CASCADE"
        case SetNull => "SET NULL"
        case SetDefault => "SET DEFAULT"
      val onUpdate = fk.onUpdate match
        case NoAction => ""
        case action => s" ON UPDATE ${renderAction(action)}"
      val onDelete = fk.onDelete match
        case NoAction => ""
        case action => s" ON DELETE ${renderAction(action)}"
      s"${nameStr}FOREIGN KEY ($cols) REFERENCES ${quoteQualifiedName(fk.refTable)}($refCols)$onUpdate$onDelete"

  given SqlRenderer[Index] with
    def toSql(idx: Index)(using config: SqlFormatConfig): String =
      val idxColRenderer = summon[SqlRenderer[IndexColumn]]
      val cols = idx.columns.map(c => idxColRenderer.toSql(c)).mkString(", ")
      val whereStr = idx.where.map(w => s" WHERE $w").getOrElse("")
      s"($cols)$whereStr"

  given SqlRenderer[Check] with
    def toSql(chk: Check)(using config: SqlFormatConfig): String =
      val nameStr = chk.name.map(n => s"CONSTRAINT ${quote(n)} ").getOrElse("")
      s"${nameStr}CHECK (${chk.expression})"

  given SqlRenderer[Unique] with
    def toSql(uniq: Unique)(using config: SqlFormatConfig): String =
      val nameStr = uniq.name.map(n => s"CONSTRAINT ${quote(n)} ").getOrElse("")
      val cols = uniq.columns.map(quote).mkString(", ")
      s"${nameStr}UNIQUE ($cols)"

  given SqlRenderer[TableConstraint] with
    def toSql(tc: TableConstraint)(using config: SqlFormatConfig): String = tc match
      case PrimaryKeyConstraint(pk) =>
        val nameStr = pk.name.map(n => s"CONSTRAINT ${quote(n)} ").getOrElse("")
        val cols = pk.columns.map(quote).mkString(", ")
        s"${nameStr}PRIMARY KEY ($cols)"
      case ForeignKeyConstraint(fk) =>
        val nameStr = fk.name.map(n => s"CONSTRAINT ${quote(n)} ").getOrElse("")
        val cols = fk.columns.map(quote).mkString(", ")
        val refCols = fk.refColumns.map(quote).mkString(", ")
        val onUpdate = fk.onUpdate match
          case NoAction => ""
          case action => s" ON UPDATE ${summon[SqlRenderer[ReferentialAction]].toSql(action)}"
        val onDelete = fk.onDelete match
          case NoAction => ""
          case action => s" ON DELETE ${summon[SqlRenderer[ReferentialAction]].toSql(action)}"
        s"${nameStr}FOREIGN KEY ($cols) REFERENCES ${quoteQualifiedName(fk.refTable)}($refCols)$onUpdate$onDelete"
      case UniqueConstraint(u) =>
        val nameStr = u.name.map(n => s"CONSTRAINT ${quote(n)} ").getOrElse("")
        val cols = u.columns.map(quote).mkString(", ")
        s"${nameStr}UNIQUE ($cols)"
      case CheckConstraint(c) =>
        val nameStr = c.name.map(n => s"CONSTRAINT ${quote(n)} ").getOrElse("")
        s"${nameStr}CHECK (${c.expression})"

  given createIndexRenderer[IO <: IndexOptions]: SqlRenderer[CreateIndex[IO]] with
    def toSql(ci: CreateIndex[IO])(using config: SqlFormatConfig): String =
      val uniqueStr = if ci.unique then "UNIQUE " else ""
      val ifNotExistsStr = if ci.ifNotExists then "IF NOT EXISTS " else ""
      val cols = ci.columns.map(c => given_SqlRenderer_IndexColumn.toSql(c)).mkString(", ")
      val whereStr = ci.options.where.map(w => s" WHERE $w").getOrElse("")
      s"CREATE ${uniqueStr}INDEX $ifNotExistsStr${quoteQualifiedName(ci.name)} ON ${quoteQualifiedName(ci.tableName)} ($cols)$whereStr"

  given SqlRenderer[DropIndex] with
    def toSql(di: DropIndex)(using config: SqlFormatConfig): String =
      val ifExistsStr = if di.ifExists then "IF EXISTS " else ""
      val cascadeStr = if di.cascade then " CASCADE" else ""
      s"DROP INDEX $ifExistsStr${quoteQualifiedName(di.name)}$cascadeStr"

  // Parameterized renderers - these use type parameters for genericity

  given columnRenderer[DT <: DataType, CO <: ColumnOptions]: SqlRenderer[Column[DT, CO]] with
    def toSql(col: Column[DT, CO])(using config: SqlFormatConfig): String =
      val nullStr = if col.nullable then "" else " NOT NULL"
      val defaultStr = col.default.map(d => s" DEFAULT $d").getOrElse("")
      val dtStr = col.dataType.toSql
      val leadingComments = config.renderLeadingComments(col.sourceComment)
      val trailingComments = config.renderTrailingComments(col.sourceComment)
      s"$leadingComments${quote(col.name)} $dtStr$defaultStr$nullStr$trailingComments"

  given tableRenderer[DT <: DataType, CO <: ColumnOptions, TO <: TableOptions](using
    colRenderer: SqlRenderer[Column[DT, CO]]
  ): SqlRenderer[Table[DT, CO, TO]] with
    def toSql(table: Table[DT, CO, TO])(using config: SqlFormatConfig): String =
      val colDefs = table.columns.map(c => colRenderer.toSql(c))
      val pkDef = table.primaryKey.map(pk => summon[SqlRenderer[PrimaryKey]].toSql(pk))
      val fkDefs = table.foreignKeys.map(fk => summon[SqlRenderer[ForeignKey]].toSql(fk))
      val uniqueDefs = table.uniques.map(u => summon[SqlRenderer[Unique]].toSql(u))
      val checkDefs = table.checks.map(c => summon[SqlRenderer[Check]].toSql(c))
      val allDefs = colDefs ++ pkDef ++ fkDefs ++ uniqueDefs ++ checkDefs
      val tableName = table.schema.map(s => s"${quote(s)}.${quote(table.name)}").getOrElse(quote(table.name))
      config.wrap(s"$tableName (", config.join(allDefs), ")")

  given createTableRenderer[DT <: DataType, CO <: ColumnOptions, TO <: TableOptions](using
    tblRenderer: SqlRenderer[Table[DT, CO, TO]]
  ): SqlRenderer[CreateTable[DT, CO, TO]] with
    def toSql(ct: CreateTable[DT, CO, TO])(using config: SqlFormatConfig): String =
      val orReplaceStr = if ct.orReplace then "OR REPLACE " else ""
      val ifNotExistsStr = if ct.ifNotExists then "IF NOT EXISTS " else ""
      val tableStr = tblRenderer.toSql(ct.table)
      val tableComments = ct.table.sourceComment
      val leadingComments =
        if tableComments.isEmpty then ""
        else if config.pretty then tableComments.map(c => config.renderComment(c)).mkString("", "\n", "\n")
        else ""
      val trailingComments =
        if tableComments.isEmpty || config.pretty then ""
        else tableComments.map(c => s"/* ${c.text} */").mkString(" ", " ", "")
      s"${leadingComments}CREATE ${orReplaceStr}TABLE $ifNotExistsStr$tableStr$trailingComments"

  given alterTableActionRenderer[DT <: DataType, CO <: ColumnOptions](using
    colRenderer: SqlRenderer[Column[DT, CO]]
  ): SqlRenderer[AlterTableAction[DT, CO]] with
    def toSql(action: AlterTableAction[DT, CO])(using config: SqlFormatConfig): String =
      val leadingComments = config.renderLeadingComments(action.sourceComment)
      val sql = action match
        case AddColumn(col, _) =>
          s"ADD COLUMN ${colRenderer.toSql(col)}"
        case DropColumn(name, cascade, _) =>
          val cascadeStr = if cascade then " CASCADE" else ""
          s"DROP COLUMN ${quote(name)}$cascadeStr"
        case ModifyColumn(col, _) =>
          s"ALTER COLUMN ${quote(col.name)} TYPE ${col.dataType.toSql}"
        case RenameColumn(oldName, newName, _) =>
          s"RENAME COLUMN ${quote(oldName)} TO ${quote(newName)}"
        case AddConstraint(constraint, _) =>
          s"ADD ${summon[SqlRenderer[TableConstraint]].toSql(constraint)}"
        case DropConstraint(name, cascade, _) =>
          val cascadeStr = if cascade then " CASCADE" else ""
          s"DROP CONSTRAINT ${quote(name)}$cascadeStr"
      s"$leadingComments$sql"

  given alterTableRenderer[DT <: DataType, CO <: ColumnOptions, TO <: TableOptions, ATA <: AlterTableAction[DT, CO]](
    using actionRenderer: SqlRenderer[AlterTableAction[DT, CO]]
  ): SqlRenderer[AlterTable[DT, CO, TO, ATA]] with
    def toSql(at: AlterTable[DT, CO, TO, ATA])(using config: SqlFormatConfig): String =
      val actions = at.actions.map(a => actionRenderer.toSql(a)).mkString(", ")
      s"ALTER TABLE ${quoteQualifiedName(at.tableName)} $actions"
