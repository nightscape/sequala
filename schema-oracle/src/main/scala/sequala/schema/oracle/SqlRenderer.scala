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

  // Oracle-specific DropTable (CASCADE CONSTRAINTS instead of CASCADE)
  given SqlRenderer[DropTable] with
    def toSql(dt: DropTable)(using config: SqlFormatConfig): String =
      val ifExistsStr = if dt.ifExists then "IF EXISTS " else ""
      val cascadeStr = if dt.cascade then " CASCADE CONSTRAINTS" else ""
      s"DROP TABLE $ifExistsStr${quote(dt.tableName)}$cascadeStr"

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
      s"${quote(col.name)} ${toOracleSql(col.dataType)}$defaultStr$nullStr$optsStr"

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
      config.wrap(s"${quote(table.name)} (", config.join(allDefs), s")$optsStr")

  given SqlRenderer[CreateOracleTable] with
    def toSql(ct: CreateOracleTable)(using config: SqlFormatConfig): String =
      val orReplaceStr = if ct.orReplace then "OR REPLACE " else ""
      val tableStr = summon[SqlRenderer[OracleTable]].toSql(ct.table)
      s"CREATE ${orReplaceStr}TABLE $tableStr"

  given SqlRenderer[OracleAlterTableAction] with
    def toSql(action: OracleAlterTableAction)(using config: SqlFormatConfig): String = action match
      case AddColumn(col) =>
        s"ADD (${summon[SqlRenderer[OracleColumn]].toSql(col)})"
      case DropColumn(name, cascade) =>
        val cascadeStr = if cascade then " CASCADE CONSTRAINTS" else ""
        s"DROP COLUMN ${quote(name)}$cascadeStr"
      case ModifyColumn(col) =>
        s"MODIFY (${summon[SqlRenderer[OracleColumn]].toSql(col)})"
      case RenameColumn(oldName, newName) =>
        s"RENAME COLUMN ${quote(oldName)} TO ${quote(newName)}"
      case AddConstraint(constraint) =>
        s"ADD ${summon[SqlRenderer[TableConstraint]].toSql(constraint)}"
      case DropConstraint(name, cascade) =>
        val cascadeStr = if cascade then " CASCADE" else ""
        s"DROP CONSTRAINT ${quote(name)}$cascadeStr"
      case AddColumns(cols) =>
        val colsSql = cols.map(c => summon[SqlRenderer[OracleColumn]].toSql(c)).mkString(", ")
        s"ADD ($colsSql)"
      case ModifyColumns(cols) =>
        val colsSql = cols.map(c => summon[SqlRenderer[OracleColumn]].toSql(c)).mkString(", ")
        s"MODIFY ($colsSql)"
      case DropColumns(names) =>
        s"DROP (${names.map(quote).mkString(", ")})"
      case AddNamedConstraint(name, constraint) =>
        s"ADD CONSTRAINT ${quote(name)} ${summon[SqlRenderer[TableConstraint]].toSql(constraint)}"

  given SqlRenderer[AlterOracleTable] with
    def toSql(at: AlterOracleTable)(using config: SqlFormatConfig): String =
      val actions = at.actions.map(a => summon[SqlRenderer[OracleAlterTableAction]].toSql(a)).mkString(" ")
      s"ALTER TABLE ${quote(at.tableName)} $actions"
