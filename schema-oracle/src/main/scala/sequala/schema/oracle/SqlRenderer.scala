package sequala.schema.oracle

import sequala.schema.{
  AddColumn,
  AddConstraint,
  BaseSqlRenderers,
  Check,
  CommonDataType,
  DataType,
  Decimal,
  DoublePrecision,
  DropColumn,
  DropConstraint,
  DropOptions,
  DropTable,
  ForeignKey,
  ModifyColumn,
  Numeric,
  PrimaryKey,
  Real,
  RenameColumn,
  SmallInt,
  SqlBigInt,
  SqlBlob,
  SqlBoolean,
  SqlChar,
  SqlClob,
  SqlDate,
  SqlFormatConfig,
  SqlInteger,
  SqlRenderer,
  SqlText,
  SqlTime,
  SqlTimestamp,
  TableConstraint,
  Unique,
  VarChar
}

object OracleSqlRenderer extends BaseSqlRenderers:

  // Oracle needs identifiers always quoted to preserve lowercase names
  override protected def quote(name: String): String = s""""$name""""

  /** Translate any DataType to Oracle-compatible SQL. CommonDataType types that Oracle doesn't support are mapped to
    * equivalents.
    */
  def toOracleSql(dt: DataType): String = dt match
    // Oracle-specific types render directly
    case o: OracleSpecificDataType => o.toSql
    // Common types that need translation for Oracle
    case SqlBigInt => "NUMBER(19)"
    case SmallInt => "NUMBER(5)"
    case SqlInteger => "NUMBER(10)"
    case Real => "BINARY_FLOAT"
    case DoublePrecision => "BINARY_DOUBLE"
    case SqlBoolean => "NUMBER(1)"
    case SqlText => "CLOB"
    case SqlBlob => "BLOB"
    case SqlClob => "CLOB"
    case SqlTime(_, _) => "TIMESTAMP"
    // Common types that Oracle supports directly
    case VarChar(len) => s"VARCHAR2($len)"
    case SqlChar(len) => s"CHAR($len)"
    case Decimal(p, s) => s"NUMBER($p, $s)"
    case Numeric(p, s) => s"NUMBER($p, $s)"
    case SqlDate => "DATE"
    case SqlTimestamp(prec, tz) =>
      val base = prec.map(p => s"TIMESTAMP($p)").getOrElse("TIMESTAMP")
      if tz then s"$base WITH TIME ZONE" else base

  given SqlRenderer[OracleDropOptions] with
    def toSql(opts: OracleDropOptions)(using config: SqlFormatConfig): String =
      val parts = Seq(
        if opts.cascadeConstraints then Some("CASCADE CONSTRAINTS") else None,
        if opts.purge then Some("PURGE") else None
      ).flatten
      if parts.isEmpty then "" else " " + parts.mkString(" ")

  // ==========================================================================
  // Oracle-specific types renderers (OracleColumn, OracleTable, etc.)
  // DataType rendering uses the OO toSql method directly
  // ==========================================================================

  given SqlRenderer[OracleColumnOptions] with
    def toSql(opts: OracleColumnOptions)(using config: SqlFormatConfig): String =
      val parts = Seq(
        opts.virtual.map(v => s"GENERATED ALWAYS AS ($v)"),
        if opts.invisible then Some("INVISIBLE") else None
      ).flatten
      if parts.isEmpty then "" else " " + parts.mkString(" ")

  given SqlRenderer[OracleColumn] with
    def toSql(col: OracleColumn)(using config: SqlFormatConfig): String =
      val nullStr = if col.nullable then "" else " NOT NULL"
      val defaultStr = col.default.map(d => s" DEFAULT $d").getOrElse("")
      val optsStr = summon[SqlRenderer[OracleColumnOptions]].toSql(col.options)
      val leadingComments = config.renderLeadingComments(col.sourceComment)
      val trailingComments = config.renderTrailingComments(col.sourceComment)
      s"$leadingComments${quote(col.name)} ${toOracleSql(col.dataType)}$defaultStr$nullStr$optsStr$trailingComments"

  given SqlRenderer[OracleStorageClause] with
    def toSql(storage: OracleStorageClause)(using config: SqlFormatConfig): String =
      val parts = Seq(
        storage.initial.map(i => s"INITIAL $i"),
        storage.next.map(n => s"NEXT $n"),
        storage.minExtents.map(m => s"MINEXTENTS $m"),
        storage.maxExtents.map(m => s"MAXEXTENTS $m"),
        storage.pctIncrease.map(p => s"PCTINCREASE $p"),
        storage.bufferPool.map(b => s"BUFFER_POOL $b")
      ).flatten
      s"STORAGE (${parts.mkString(" ")})"

  given SqlRenderer[OracleTableOptions] with
    def toSql(opts: OracleTableOptions)(using config: SqlFormatConfig): String =
      val parts = Seq(
        opts.tablespace.map(t => s"TABLESPACE $t"),
        opts.pctFree.map(p => s"PCTFREE $p"),
        opts.pctUsed.map(p => s"PCTUSED $p"),
        opts.iniTrans.map(i => s"INITRANS $i"),
        opts.maxTrans.map(m => s"MAXTRANS $m"),
        opts.storage.map(s => summon[SqlRenderer[OracleStorageClause]].toSql(s)),
        opts.logging.map(l => if l then "LOGGING" else "NOLOGGING"),
        opts.compress.map {
          case OracleCompressBasic => "COMPRESS BASIC"
          case OracleCompressAdvanced => "COMPRESS FOR OLTP"
          case OracleCompressFor(op) => s"COMPRESS FOR $op"
        },
        opts.cache.map(c => if c then "CACHE" else "NOCACHE"),
        opts.parallel.map { p =>
          p.degree.map(d => s"PARALLEL $d").getOrElse("PARALLEL")
        },
        opts.rowMovement.map(r => if r then "ENABLE ROW MOVEMENT" else "DISABLE ROW MOVEMENT")
      ).flatten
      if parts.isEmpty then "" else " " + parts.mkString(" ")

  given SqlRenderer[OracleTable] with
    def toSql(table: OracleTable)(using config: SqlFormatConfig): String =
      val colDefs = table.columns.map(c => summon[SqlRenderer[OracleColumn]].toSql(c))
      val pkDef = table.primaryKey.map(pk => summon[SqlRenderer[PrimaryKey]].toSql(pk))
      val fkDefs = table.foreignKeys.map(fk => summon[SqlRenderer[ForeignKey]].toSql(fk))
      val uniqueDefs = table.uniques.map(u => summon[SqlRenderer[Unique]].toSql(u))
      val checkDefs = table.checks.map(c => summon[SqlRenderer[Check]].toSql(c))
      val allDefs = colDefs ++ pkDef ++ fkDefs ++ uniqueDefs ++ checkDefs
      val optsStr = summon[SqlRenderer[OracleTableOptions]].toSql(table.options)
      val tableName = table.schema.map(s => s"${quote(s)}.${quote(table.name)}").getOrElse(quote(table.name))
      config.wrap(s"$tableName (", config.join(allDefs), s")$optsStr")

  given SqlRenderer[CreateOracleTable] with
    def toSql(ct: CreateOracleTable)(using config: SqlFormatConfig): String =
      val orReplaceStr = if ct.orReplace then "OR REPLACE " else ""
      val tableStr = summon[SqlRenderer[OracleTable]].toSql(ct.table)
      val tableComments = ct.table.sourceComment
      val leadingComments =
        if tableComments.isEmpty then ""
        else if config.pretty then tableComments.map(c => config.renderComment(c)).mkString("", "\n", "\n")
        else ""
      val trailingComments =
        if tableComments.isEmpty || config.pretty then ""
        else tableComments.map(c => s"/* ${c.text} */").mkString(" ", " ", "")
      s"${leadingComments}CREATE ${orReplaceStr}TABLE $tableStr$trailingComments"

  given SqlRenderer[OracleAlterTableAction] with
    def toSql(action: OracleAlterTableAction)(using config: SqlFormatConfig): String =
      val leadingComments = config.renderLeadingComments(action.sourceComment)
      val sql = action match
        case AddColumn(col, _) =>
          s"ADD (${summon[SqlRenderer[OracleColumn]].toSql(col)})"
        case DropColumn(name, cascade, _) =>
          val cascadeStr = if cascade then " CASCADE CONSTRAINTS" else ""
          s"DROP COLUMN ${quote(name)}$cascadeStr"
        case ModifyColumn(col, _) =>
          s"MODIFY (${summon[SqlRenderer[OracleColumn]].toSql(col)})"
        case RenameColumn(oldName, newName, _) =>
          s"RENAME COLUMN ${quote(oldName)} TO ${quote(newName)}"
        case AddConstraint(constraint, _) =>
          s"ADD ${summon[SqlRenderer[TableConstraint]].toSql(constraint)}"
        case DropConstraint(name, cascade, _) =>
          val cascadeStr = if cascade then " CASCADE" else ""
          s"DROP CONSTRAINT ${quote(name)}$cascadeStr"
        case AddColumns(cols, _) =>
          val colsSql = cols.map(c => summon[SqlRenderer[OracleColumn]].toSql(c)).mkString(", ")
          s"ADD ($colsSql)"
        case ModifyColumns(cols, _) =>
          val colsSql = cols.map(c => summon[SqlRenderer[OracleColumn]].toSql(c)).mkString(", ")
          s"MODIFY ($colsSql)"
        case DropColumns(names, _) =>
          s"DROP (${names.map(quote).mkString(", ")})"
        case AddNamedConstraint(name, constraint, _) =>
          s"ADD CONSTRAINT ${quote(name)} ${summon[SqlRenderer[TableConstraint]].toSql(constraint)}"
      s"$leadingComments$sql"

  given SqlRenderer[AlterOracleTable] with
    def toSql(at: AlterOracleTable)(using config: SqlFormatConfig): String =
      val actions = at.actions.map(a => summon[SqlRenderer[OracleAlterTableAction]].toSql(a)).mkString(" ")
      s"ALTER TABLE ${quoteQualifiedName(at.tableName)} $actions"

  given dropTableRenderer[DO <: DropOptions](using optsRenderer: SqlRenderer[DO]): SqlRenderer[DropTable[DO]] with
    def toSql(dt: DropTable[DO])(using config: SqlFormatConfig): String =
      // Oracle does not support IF EXISTS in DROP TABLE (only Oracle 23c+)
      // For older Oracle versions, callers should handle ORA-00942 errors
      val optsStr = optsRenderer.toSql(dt.options)
      s"DROP TABLE ${quoteQualifiedName(dt.tableName)}$optsStr"

  given SqlRenderer[Grant] with
    def toSql(g: Grant)(using config: SqlFormatConfig): String =
      val privs = g.privileges.mkString(", ")
      val users = g.toUsers.map(_.name).mkString(", ")
      val grant = if g.withGrantOption then " WITH GRANT OPTION" else ""
      s"GRANT $privs ON ${g.onObject.name} TO $users$grant"

  given SqlRenderer[Revoke] with
    def toSql(r: Revoke)(using config: SqlFormatConfig): String =
      val privs = r.privileges.mkString(", ")
      val users = r.fromUsers.map(_.name).mkString(", ")
      s"REVOKE $privs ON ${r.onObject.name} FROM $users"

  given SqlRenderer[OracleColumnComment] with
    def toSql(cc: OracleColumnComment)(using config: SqlFormatConfig): String =
      val text = cc.commentText.replace("'", "''")
      s"COMMENT ON COLUMN ${cc.qualifiedColumnName.name} IS '$text'"

  given SqlRenderer[OracleTableComment] with
    def toSql(tc: OracleTableComment)(using config: SqlFormatConfig): String =
      val text = tc.commentText.replace("'", "''")
      s"COMMENT ON TABLE ${tc.tableNameParsed.name} IS '$text'"

  given SqlRenderer[UserQuota] with
    def toSql(q: UserQuota)(using config: SqlFormatConfig): String =
      val quotaStr = q.quota.getOrElse("UNLIMITED")
      s"QUOTA $quotaStr ON ${q.tablespace}"

  given SqlRenderer[CreateTablespace] with
    def toSql(ct: CreateTablespace)(using config: SqlFormatConfig): String =
      val datafileClause = ct.datafile.map(f => s" DATAFILE '$f'").getOrElse(s" DATAFILE '${ct.name}.dbf'")
      val sizeClause = ct.size.map(s => s" SIZE $s").getOrElse(" SIZE 100M")
      val autoextendClause = if ct.autoextend then " AUTOEXTEND ON" else ""
      s"CREATE TABLESPACE ${ct.name}$datafileClause$sizeClause$autoextendClause"

  given SqlRenderer[CreateUser] with
    def toSql(cu: CreateUser)(using config: SqlFormatConfig): String =
      val quotaRenderer = summon[SqlRenderer[UserQuota]]
      val quotaClauses = cu.quotas.map(q => quotaRenderer.toSql(q)).mkString(" ")
      val defaultTs = cu.defaultTablespace.map(t => s" DEFAULT TABLESPACE $t").getOrElse("")
      val quotaStr = if quotaClauses.nonEmpty then s" $quotaClauses" else ""
      s"CREATE USER ${cu.name} IDENTIFIED BY ${cu.identifiedBy}$defaultTs$quotaStr"
