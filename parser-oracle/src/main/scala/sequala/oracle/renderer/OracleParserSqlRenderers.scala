package sequala.oracle.renderer

import sequala.schema.ast.{AlterViewAction, Name, SelectBody}
import sequala.schema.ast.Expression
import sequala.common.statement.*
import sequala.schema.statement.*
import sequala.oracle.*
import sequala.common.renderer.ParserSqlRenderers.given
import sequala.schema.{
  AlterTable,
  AlterView,
  CreateIndex,
  CreateTable,
  CreateView,
  Delete,
  DropView,
  Explain,
  Insert,
  Select,
  SqlFormatConfig,
  SqlRenderer,
  Update
}
import sequala.schema.oracle.{
  CreateTablespace,
  CreateUser,
  OracleCreateViewOptions,
  OracleDialect,
  OracleDropOptions,
  OracleTableOptions,
  UserQuota
}

abstract class OracleParserSqlRenderers extends sequala.common.renderer.ParserSqlRenderers, OracleDialect:

  override def renderStatement(stmt: sequala.schema.Statement)(using config: SqlFormatConfig): String =
    stmt match
      case os: OracleStatement => OracleParserSqlRenderers.oracleStatementRenderer.toSql(os)
      case cv: CreateView[CreateViewOptions @unchecked] =>
        OracleParserSqlRenderers.oracleCreateViewRenderer.toSql(cv)
      case ct: CreateTable[?, ?, ?] =>
        // Use OracleSqlRenderer for Oracle-style identifier quoting
        import sequala.schema.oracle.OracleSqlRenderer.given
        import sequala.schema.SqlRenderer.toSql
        ct.asInstanceOf[sequala.schema.oracle.CreateOracleTable].toSql + ";"
      case dt: sequala.schema.DropTable[?] =>
        // Use OracleSqlRenderer for Oracle-style identifier quoting
        import sequala.schema.oracle.OracleSqlRenderer.given
        import sequala.schema.SqlRenderer.toSql
        dt.asInstanceOf[sequala.schema.DropTable[sequala.schema.oracle.OracleDropOptions]].toSql + ";"
      case other => super.renderStatement(other)

object OracleParserSqlRenderers extends OracleParserSqlRenderers:
  import sequala.schema.oracle.OracleSqlRenderer.{given_SqlRenderer_CreateTablespace, given_SqlRenderer_CreateUser}

  given dropOptionsRenderer: SqlRenderer[DropOptions] =
    sequala.schema.oracle.OracleSqlRenderer.given_SqlRenderer_OracleDropOptions

  given SqlRenderer[OracleAlterView] with
    def toSql(oav: OracleAlterView)(using config: SqlFormatConfig): String =
      summon[SqlRenderer[AlterView]].toSql(AlterView(oav.name, oav.action))

  given (using nameRenderer: SqlRenderer[Name], annRenderer: SqlRenderer[ColumnAnnotation]): SqlRenderer[
    ColumnModification
  ] with
    def toSql(cm: ColumnModification)(using config: SqlFormatConfig): String =
      val typeStr = cm.dataType.map(" " + nameRenderer.toSql(_)).getOrElse("")
      val anns = cm.annotations.map(annRenderer.toSql).mkString(" ")
      s"${nameRenderer.toSql(cm.column)}$typeStr $anns".trim

  given (using modRenderer: SqlRenderer[ColumnModification]): SqlRenderer[AlterTableModify] with
    def toSql(atm: AlterTableModify)(using config: SqlFormatConfig): String =
      s"MODIFY(${atm.modifications.map(modRenderer.toSql).mkString(", ")})"

  given (using colRenderer: SqlRenderer[ColumnDefinition], annRenderer: SqlRenderer[TableAnnotation]): SqlRenderer[
    AlterTableAdd
  ] with
    def toSql(ata: AlterTableAdd)(using config: SqlFormatConfig): String =
      val parts = (ata.columns.map(colRenderer.toSql) ++ ata.annotations.map(annRenderer.toSql)).mkString(", ")
      s"ADD $parts"

  given alterTableActionRenderer: SqlRenderer[ParserAlterTableAction] with
    def toSql(action: ParserAlterTableAction)(using config: SqlFormatConfig): String =
      val modRenderer = summon[SqlRenderer[ColumnModification]]
      val colRenderer = summon[SqlRenderer[ColumnDefinition]]
      val annRenderer = summon[SqlRenderer[TableAnnotation]]
      action match
        case AlterTableModify(modifications) =>
          s"MODIFY(${modifications.map(modRenderer.toSql).mkString(", ")})"
        case AlterTableAdd(columns, annotations) =>
          val parts = (columns.map(colRenderer.toSql) ++ annotations.map(annRenderer.toSql)).mkString(", ")
          s"ADD $parts"
        case other: sequala.schema.AlterTableAction[?, ?] =>
          // Delegate to schema renderer for standard actions
          import sequala.schema.oracle.{OracleColumnOptions, OracleDataType}
          import sequala.schema.given
          summon[SqlRenderer[sequala.schema.AlterTableAction[OracleDataType, OracleColumnOptions]]]
            .toSql(other.asInstanceOf[sequala.schema.AlterTableAction[OracleDataType, OracleColumnOptions]])

  given oracleCreateViewRenderer(using
    nameRenderer: SqlRenderer[Name],
    queryRenderer: SqlRenderer[SelectBody]
  ): SqlRenderer[CreateView[OracleCreateViewOptions]] with
    def toSql(cv: CreateView[OracleCreateViewOptions])(using config: SqlFormatConfig): String =
      val opts = cv.options
      val forceStr = if opts.force then " FORCE" else ""
      val editionableStr = if opts.editionable then " EDITIONABLE" else ""
      val colListStr = opts.columnList.map(cols => s" (${cols.map(nameRenderer.toSql).mkString(", ")})").getOrElse("")
      val orReplaceStr = if cv.orReplace then "OR REPLACE" else ""
      s"CREATE $orReplaceStr$forceStr$editionableStr VIEW ${nameRenderer.toSql(cv.name)}$colListStr AS ${queryRenderer.toSql(cv.query)};"

  given SqlRenderer[StorageClause] with
    def toSql(sc: StorageClause)(using config: SqlFormatConfig): String =
      val options = Seq(
        sc.initial.map(i => s"INITIAL $i"),
        sc.next.map(n => s"NEXT $n"),
        sc.minExtents.map(m => s"MINEXTENTS $m"),
        sc.maxExtents.map(m => s"MAXEXTENTS $m"),
        sc.pctIncrease.map(p => s"PCTINCREASE $p"),
        sc.bufferPool.map(b => s"BUFFER_POOL $b")
      ).flatten.mkString(" ")
      s"STORAGE ($options)"

  // OracleCreateTable renderer - delegates to schema-level renderer
  // The schema-level renderer will use OracleTableOptions renderer automatically
  given SqlRenderer[OracleCreateTable] with
    def toSql(oct: OracleCreateTable)(using config: SqlFormatConfig): String =
      import sequala.schema.oracle.given
      import sequala.schema.given
      summon[SqlRenderer[CreateTable[
        sequala.schema.oracle.OracleDataType,
        sequala.schema.oracle.OracleColumnOptions,
        sequala.schema.oracle.OracleTableOptions
      ]]].toSql(oct)

  given SqlRenderer[Prompt] with
    def toSql(p: Prompt)(using config: SqlFormatConfig): String = s"PROMPT ${p.message}"

  given SqlRenderer[Commit] with
    def toSql(c: Commit)(using config: SqlFormatConfig): String = "COMMIT"

  given SqlRenderer[SetDefineOff] with
    def toSql(sdo: SetDefineOff)(using config: SqlFormatConfig): String = "SET DEFINE OFF"

  given SqlRenderer[SetScanOff] with
    def toSql(sso: SetScanOff)(using config: SqlFormatConfig): String = "SET SCAN OFF"

  given SqlRenderer[SetScanOn] with
    def toSql(sso: SetScanOn)(using config: SqlFormatConfig): String = "SET SCAN ON"

  given (using nameRenderer: SqlRenderer[Name]): SqlRenderer[Grant] with
    def toSql(g: Grant)(using config: SqlFormatConfig): String =
      val grantOptStr = if g.withGrantOption then " WITH GRANT OPTION" else ""
      s"GRANT ${g.privileges.mkString(", ")} ON ${nameRenderer.toSql(g.onObject)} TO ${g.toUsers.map(nameRenderer.toSql).mkString(", ")}$grantOptStr;"

  given (using nameRenderer: SqlRenderer[Name]): SqlRenderer[Revoke] with
    def toSql(r: Revoke)(using config: SqlFormatConfig): String =
      s"REVOKE ${r.privileges.mkString(", ")} ON ${nameRenderer.toSql(r.onObject)} FROM ${r.fromUsers.map(nameRenderer.toSql).mkString(", ")};"

  given (using nameRenderer: SqlRenderer[Name]): SqlRenderer[OracleCreateSynonym] with
    def toSql(ocs: OracleCreateSynonym)(using config: SqlFormatConfig): String =
      val orReplaceStr = if ocs.orReplace then "OR REPLACE " else ""
      s"CREATE ${orReplaceStr}SYNONYM ${nameRenderer.toSql(ocs.name)} FOR ${nameRenderer.toSql(ocs.target)};"

  given (using nameRenderer: SqlRenderer[Name]): SqlRenderer[OracleDropSynonym] with
    def toSql(ods: OracleDropSynonym)(using config: SqlFormatConfig): String =
      val ifExistsStr = if ods.ifExists then "IF EXISTS " else ""
      s"DROP SYNONYM $ifExistsStr${nameRenderer.toSql(ods.name)};"

  given (using nameRenderer: SqlRenderer[Name]): SqlRenderer[OracleRename] with
    def toSql(or: OracleRename)(using config: SqlFormatConfig): String =
      s"RENAME ${nameRenderer.toSql(or.oldName)} TO ${nameRenderer.toSql(or.newName)};"

  given SqlRenderer[PlSqlBlock] with
    def toSql(psb: PlSqlBlock)(using config: SqlFormatConfig): String =
      s"BEGIN\n${psb.content}\nEND;"

  given (using nameRenderer: SqlRenderer[Name], queryRenderer: SqlRenderer[SelectBody]): SqlRenderer[PlSqlForLoop] with
    def toSql(psfl: PlSqlForLoop)(using config: SqlFormatConfig): String =
      s"FOR ${nameRenderer.toSql(psfl.variable)} IN (${queryRenderer.toSql(psfl.query)}) LOOP\n${psfl.content}\nEND LOOP;"

  given SqlRenderer[PlSqlWhileLoop] with
    def toSql(pswl: PlSqlWhileLoop)(using config: SqlFormatConfig): String =
      s"WHILE ${pswl.condition} LOOP\n${pswl.content}\nEND LOOP;"

  given (using nameRenderer: SqlRenderer[Name]): SqlRenderer[Comment] with
    def toSql(c: Comment)(using config: SqlFormatConfig): String =
      val escapedComment = c.commentText.replace("'", "''")
      s"COMMENT ON COLUMN ${nameRenderer.toSql(c.qualifiedColumnName)} IS '$escapedComment';"

  given (using nameRenderer: SqlRenderer[Name]): SqlRenderer[TableComment] with
    def toSql(tc: TableComment)(using config: SqlFormatConfig): String =
      val escapedComment = tc.commentText.replace("'", "''")
      s"COMMENT ON TABLE ${nameRenderer.toSql(tc.tableNameParsed)} IS '$escapedComment';"

  // Main OracleStatement renderer (dispatches to specific renderers)
  given oracleStatementRenderer: SqlRenderer[OracleStatement] with
    def toSql(os: OracleStatement)(using config: SqlFormatConfig): String = os match
      case oav: OracleAlterView => summon[SqlRenderer[OracleAlterView]].toSql(oav)
      case p: Prompt => summon[SqlRenderer[Prompt]].toSql(p)
      case c: Commit => summon[SqlRenderer[Commit]].toSql(c)
      case sdo: SetDefineOff => summon[SqlRenderer[SetDefineOff]].toSql(sdo)
      case sso: SetScanOff => summon[SqlRenderer[SetScanOff]].toSql(sso)
      case sso: SetScanOn => summon[SqlRenderer[SetScanOn]].toSql(sso)
      case g: Grant => summon[SqlRenderer[Grant]].toSql(g)
      case r: Revoke => summon[SqlRenderer[Revoke]].toSql(r)
      case ocs: OracleCreateSynonym => summon[SqlRenderer[OracleCreateSynonym]].toSql(ocs)
      case ods: OracleDropSynonym => summon[SqlRenderer[OracleDropSynonym]].toSql(ods)
      case or: OracleRename => summon[SqlRenderer[OracleRename]].toSql(or)
      case psb: PlSqlBlock => summon[SqlRenderer[PlSqlBlock]].toSql(psb)
      case psfl: PlSqlForLoop => summon[SqlRenderer[PlSqlForLoop]].toSql(psfl)
      case pswl: PlSqlWhileLoop => summon[SqlRenderer[PlSqlWhileLoop]].toSql(pswl)
      case c: Comment => summon[SqlRenderer[Comment]].toSql(c)
      case tc: TableComment => summon[SqlRenderer[TableComment]].toSql(tc)
      case ct: CreateTablespace => summon[SqlRenderer[CreateTablespace]].toSql(ct)
      case cu: CreateUser => summon[SqlRenderer[CreateUser]].toSql(cu)
      case _ => throw new MatchError(s"Unsupported Oracle statement type: ${os.getClass}")

  // Oracle-specific SqlRenderers for CommonDataType that produce Oracle SQL syntax (e.g. VARCHAR2 instead of VARCHAR)
  import sequala.schema.oracle.OracleSqlRenderer.toOracleSql
  import sequala.schema.GenericSqlRenderer.{
    given_SqlRenderer_Check,
    given_SqlRenderer_ForeignKey,
    given_SqlRenderer_PrimaryKey,
    given_SqlRenderer_TableConstraint,
    given_SqlRenderer_Unique
  }
  import sequala.schema.{
    AddColumn,
    AddConstraint,
    AlterTableAction,
    Check,
    Column,
    CommonDataType,
    DropColumn,
    DropConstraint,
    ForeignKey,
    ModifyColumn,
    NoColumnOptions,
    NoTableOptions,
    PrimaryKey,
    RenameColumn,
    Table,
    TableConstraint,
    Unique
  }

  private def quoteCommon(name: String): String = s""""$name""""
  private def quoteQualifiedNameCommon(name: String): String = name.split('.').map(quoteCommon).mkString(".")

  given commonColumnRenderer: SqlRenderer[Column[CommonDataType, NoColumnOptions.type]] with
    def toSql(col: Column[CommonDataType, NoColumnOptions.type])(using config: SqlFormatConfig): String =
      val nullStr = if col.nullable then "" else " NOT NULL"
      val defaultStr = col.default.map(d => s" DEFAULT $d").getOrElse("")
      val leadingComments = config.renderLeadingComments(col.sourceComment)
      val trailingComments = config.renderTrailingComments(col.sourceComment)
      s"$leadingComments${quoteCommon(col.name)} ${toOracleSql(col.dataType)}$defaultStr$nullStr$trailingComments"

  given commonTableRenderer: SqlRenderer[Table[CommonDataType, NoColumnOptions.type, NoTableOptions.type]] with
    def toSql(table: Table[CommonDataType, NoColumnOptions.type, NoTableOptions.type])(using
      config: SqlFormatConfig
    ): String =
      val colDefs = table.columns.map(c => commonColumnRenderer.toSql(c))
      val pkDef = table.primaryKey.map(pk => summon[SqlRenderer[PrimaryKey]].toSql(pk))
      val fkDefs = table.foreignKeys.map(fk => summon[SqlRenderer[ForeignKey]].toSql(fk))
      val uniqueDefs = table.uniques.map(u => summon[SqlRenderer[Unique]].toSql(u))
      val checkDefs = table.checks.map(c => summon[SqlRenderer[Check]].toSql(c))
      val allDefs = colDefs ++ pkDef ++ fkDefs ++ uniqueDefs ++ checkDefs
      val tableName =
        table.schema.map(s => s"${quoteCommon(s)}.${quoteCommon(table.name)}").getOrElse(quoteCommon(table.name))
      config.wrap(s"$tableName (", config.join(allDefs), ")")

  given commonCreateTableRenderer: SqlRenderer[CreateTable[CommonDataType, NoColumnOptions.type, NoTableOptions.type]]
  with
    def toSql(ct: CreateTable[CommonDataType, NoColumnOptions.type, NoTableOptions.type])(using
      config: SqlFormatConfig
    ): String =
      val orReplaceStr = if ct.orReplace then "OR REPLACE " else ""
      val ifNotExistsStr = if ct.ifNotExists then "IF NOT EXISTS " else ""
      s"CREATE ${orReplaceStr}TABLE $ifNotExistsStr${commonTableRenderer.toSql(ct.table)}"

  given commonActionRenderer: SqlRenderer[AlterTableAction[CommonDataType, NoColumnOptions.type]] with
    def toSql(action: AlterTableAction[CommonDataType, NoColumnOptions.type])(using config: SqlFormatConfig): String =
      val leadingComments = config.renderLeadingComments(action.sourceComment)
      val sql = action match
        case AddColumn(col, _) =>
          s"ADD (${commonColumnRenderer.toSql(col)})"
        case DropColumn(name, cascade, _) =>
          val cascadeStr = if cascade then " CASCADE CONSTRAINTS" else ""
          s"DROP COLUMN ${quoteCommon(name)}$cascadeStr"
        case ModifyColumn(col, _) =>
          s"MODIFY (${commonColumnRenderer.toSql(col)})"
        case RenameColumn(oldName, newName, _) =>
          s"RENAME COLUMN ${quoteCommon(oldName)} TO ${quoteCommon(newName)}"
        case AddConstraint(constraint, _) =>
          s"ADD ${summon[SqlRenderer[TableConstraint]].toSql(constraint)}"
        case DropConstraint(name, cascade, _) =>
          val cascadeStr = if cascade then " CASCADE" else ""
          s"DROP CONSTRAINT ${quoteCommon(name)}$cascadeStr"
      s"$leadingComments$sql"

  given commonAlterTableRenderer: SqlRenderer[AlterTable[
    CommonDataType,
    NoColumnOptions.type,
    NoTableOptions.type,
    AlterTableAction[CommonDataType, NoColumnOptions.type]
  ]] with
    def toSql(
      at: AlterTable[
        CommonDataType,
        NoColumnOptions.type,
        NoTableOptions.type,
        AlterTableAction[CommonDataType, NoColumnOptions.type]
      ]
    )(using config: SqlFormatConfig): String =
      val actions = at.actions.map(a => commonActionRenderer.toSql(a)).mkString(" ")
      s"ALTER TABLE ${quoteQualifiedNameCommon(at.tableName)} $actions"
