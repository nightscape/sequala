package sequala.schema.postgres

import sequala.schema.{
  AddColumn,
  AddConstraint,
  BaseSqlRenderers,
  Check,
  DropColumn,
  DropConstraint,
  DropTable,
  ForeignKey,
  ModifyColumn,
  PrimaryKey,
  RenameColumn,
  SqlFormatConfig,
  SqlRenderer,
  TableConstraint,
  Unique
}

object PostgresSqlRenderer extends BaseSqlRenderers:

  // ==========================================================================
  // Postgres-specific types renderers (PostgresColumn, PostgresTable, etc.)
  // DataType rendering uses the OO toSql method directly
  // Base renderers for constraints, indexes, etc. are inherited from BaseSqlRenderers
  // ==========================================================================

  given SqlRenderer[PostgresGeneratedColumn] with
    def toSql(gen: PostgresGeneratedColumn)(using config: SqlFormatConfig): String =
      val storedStr = if gen.stored then " STORED" else ""
      s"GENERATED ALWAYS AS (${gen.expression})$storedStr"

  given SqlRenderer[PostgresColumnOptions] with
    def toSql(opts: PostgresColumnOptions)(using config: SqlFormatConfig): String =
      val parts = Seq(
        opts.generatedAs.map(g => summon[SqlRenderer[PostgresGeneratedColumn]].toSql(g)),
        opts.compression.map(c => s"COMPRESSION $c")
      ).flatten
      if parts.isEmpty then "" else " " + parts.mkString(" ")

  given SqlRenderer[PostgresColumn] with
    def toSql(col: PostgresColumn)(using config: SqlFormatConfig): String =
      val nullStr = if col.nullable then "" else " NOT NULL"
      val defaultStr = col.default.map(d => s" DEFAULT $d").getOrElse("")
      val optsStr = summon[SqlRenderer[PostgresColumnOptions]].toSql(col.options)
      s"${quote(col.name)} ${col.dataType.toSql}$defaultStr$nullStr$optsStr"

  given SqlRenderer[PostgresPartitionSpec] with
    def toSql(spec: PostgresPartitionSpec)(using config: SqlFormatConfig): String =
      val stratStr = spec.strategy match
        case PostgresPartitionByRange => "RANGE"
        case PostgresPartitionByList => "LIST"
        case PostgresPartitionByHash => "HASH"
      val cols = spec.columns.map(quote).mkString(", ")
      s"PARTITION BY $stratStr ($cols)"

  given SqlRenderer[PostgresTableOptions] with
    def toSql(opts: PostgresTableOptions)(using config: SqlFormatConfig): String =
      val parts = Seq(
        if opts.inherits.nonEmpty then Some(s"INHERITS (${opts.inherits.map(quote).mkString(", ")})")
        else None,
        opts.partitionBy.map(p => summon[SqlRenderer[PostgresPartitionSpec]].toSql(p)),
        opts.using.map(u => s"USING $u"),
        if opts.withOptions.nonEmpty then Some(s"WITH (${opts.withOptions.map((k, v) => s"$k = $v").mkString(", ")})")
        else None
      ).flatten
      if parts.isEmpty then "" else " " + parts.mkString(" ")

  given SqlRenderer[PostgresTable] with
    def toSql(table: PostgresTable)(using config: SqlFormatConfig): String =
      val colDefs = table.columns.map(c => summon[SqlRenderer[PostgresColumn]].toSql(c))
      val pkDef = table.primaryKey.map(pk => summon[SqlRenderer[PrimaryKey]].toSql(pk))
      val fkDefs = table.foreignKeys.map(fk => summon[SqlRenderer[ForeignKey]].toSql(fk))
      val uniqueDefs = table.uniques.map(u => summon[SqlRenderer[Unique]].toSql(u))
      val checkDefs = table.checks.map(c => summon[SqlRenderer[Check]].toSql(c))
      val allDefs = colDefs ++ pkDef ++ fkDefs ++ uniqueDefs ++ checkDefs
      val optsStr = summon[SqlRenderer[PostgresTableOptions]].toSql(table.options)
      config.wrap(s"${quote(table.name)} (", config.join(allDefs), s")$optsStr")

  given SqlRenderer[CreatePostgresTable] with
    def toSql(ct: CreatePostgresTable)(using config: SqlFormatConfig): String =
      val orReplaceStr = if ct.orReplace then "OR REPLACE " else ""
      val ifNotExistsStr = if ct.ifNotExists then "IF NOT EXISTS " else ""
      val tableStr = summon[SqlRenderer[PostgresTable]].toSql(ct.table)
      s"CREATE ${orReplaceStr}TABLE $ifNotExistsStr$tableStr"

  given SqlRenderer[DropTable] with
    def toSql(dt: DropTable)(using config: SqlFormatConfig): String =
      val ifExistsStr = if dt.ifExists then "IF EXISTS " else ""
      val cascadeStr = if dt.cascade then " CASCADE" else ""
      s"DROP TABLE $ifExistsStr${quote(dt.tableName)}$cascadeStr"

  given SqlRenderer[PostgresAlterTableAction] with
    def toSql(action: PostgresAlterTableAction)(using config: SqlFormatConfig): String = action match
      case AddColumn(col) =>
        s"ADD COLUMN ${summon[SqlRenderer[PostgresColumn]].toSql(col)}"
      case DropColumn(name, cascade) =>
        val cascadeStr = if cascade then " CASCADE" else ""
        s"DROP COLUMN ${quote(name)}$cascadeStr"
      case ModifyColumn(col) =>
        s"ALTER COLUMN ${quote(col.name)} TYPE ${col.dataType.toSql}"
      case RenameColumn(oldName, newName) =>
        s"RENAME COLUMN ${quote(oldName)} TO ${quote(newName)}"
      case AddConstraint(constraint) =>
        s"ADD ${summon[SqlRenderer[TableConstraint]].toSql(constraint)}"
      case DropConstraint(name, cascade) =>
        val cascadeStr = if cascade then " CASCADE" else ""
        s"DROP CONSTRAINT ${quote(name)}$cascadeStr"

  given SqlRenderer[AlterPostgresTable] with
    def toSql(at: AlterPostgresTable)(using config: SqlFormatConfig): String =
      val actions = at.actions.map(a => summon[SqlRenderer[PostgresAlterTableAction]].toSql(a)).mkString(", ")
      s"ALTER TABLE ${quote(at.tableName)} $actions"
