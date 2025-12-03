package sequala.schema.oracle

import sequala.schema.{
  AlterTable,
  AlterTableAction,
  ColumnCommentStatement,
  CreateIndex,
  CreateTable,
  DataType,
  DropColumn,
  DropConstraint,
  DropTable,
  DropView,
  IndexColumn,
  NoColumnOptions,
  RenameColumn,
  Statement,
  TableCommentStatement,
  TableConstraint
}
import sequala.schema.ast.{AlterViewAction, Name, SelectBody}
import sequala.schema.statement.{ColumnAnnotation, ColumnDefinition, TableAnnotation}

/** Oracle-specific statement types. Extends the base Statement ADT with Oracle-specific features.
  */
sealed trait OracleStatement extends Statement

/** Oracle ALTER VIEW statement.
  */
case class OracleAlterView(name: Name, action: AlterViewAction) extends OracleStatement

/** Oracle PROMPT statement (SQL*Plus directive).
  */
case class Prompt(message: String) extends OracleStatement

/** Oracle COMMIT statement.
  */
case class Commit() extends OracleStatement

/** Oracle SET DEFINE OFF statement (SQL*Plus directive).
  */
case class SetDefineOff() extends OracleStatement

/** Oracle SET SCAN OFF statement (SQL*Plus directive).
  */
case class SetScanOff() extends OracleStatement

/** Oracle SET SCAN ON statement (SQL*Plus directive).
  */
case class SetScanOn() extends OracleStatement

/** Oracle STORAGE clause for CREATE TABLE.
  */
case class StorageClause(
  initial: Option[String] = None,
  next: Option[String] = None,
  minExtents: Option[Int] = None,
  maxExtents: Option[String] = None, // "UNLIMITED" or number
  pctIncrease: Option[Int] = None,
  bufferPool: Option[String] = None
)

/** Oracle CREATE TABLESPACE statement.
  */
case class CreateTablespace(
  name: String,
  datafile: Option[String] = None,
  size: Option[String] = None,
  autoextend: Boolean = true
) extends OracleStatement

/** Oracle CREATE USER statement with quotas.
  */
case class CreateUser(
  name: String,
  identifiedBy: String,
  defaultTablespace: Option[String] = None,
  quotas: Seq[UserQuota] = Seq.empty,
  roles: Seq[String] = Seq("CONNECT", "RESOURCE")
) extends OracleStatement

/** User quota on a tablespace.
  */
case class UserQuota(tablespace: String, quota: Option[String] = None) // None = UNLIMITED

/** Oracle GRANT statement.
  */
case class Grant(privileges: Seq[String], onObject: Name, toUsers: Seq[Name], withGrantOption: Boolean = false)
    extends OracleStatement

/** Oracle REVOKE statement.
  */
case class Revoke(privileges: Seq[String], onObject: Name, fromUsers: Seq[Name]) extends OracleStatement

/** Oracle CREATE SYNONYM statement.
  */
case class OracleCreateSynonym(name: Name, orReplace: Boolean, target: Name) extends OracleStatement

/** Oracle DROP SYNONYM statement.
  */
case class OracleDropSynonym(name: Name, ifExists: Boolean) extends OracleStatement

/** Oracle RENAME statement (standalone, not ALTER TABLE).
  */
case class OracleRename(oldName: Name, newName: Name) extends OracleStatement

/** PL/SQL BEGIN...END block. Content is stored as a string since it may contain procedural code.
  */
case class PlSqlBlock(content: String) extends OracleStatement

/** PL/SQL FOR...LOOP...END LOOP block.
  */
case class PlSqlForLoop(variable: Name, query: SelectBody, content: String) extends OracleStatement

/** PL/SQL WHILE...LOOP...END LOOP block.
  */
case class PlSqlWhileLoop(condition: String, content: String) extends OracleStatement

/** Oracle COMMENT ON COLUMN statement.
  */
case class OracleColumnComment(qualifiedColumnName: Name, commentText: String)
    extends OracleStatement
    with ColumnCommentStatement:

  override def tableName: String =
    val parts = qualifiedColumnName.name.split("\\.")
    if parts.length >= 2 then parts(parts.length - 2) else ""

  override def columnName: String =
    val parts = qualifiedColumnName.name.split("\\.")
    parts.last

  override def comment: String = commentText

/** Oracle COMMENT ON TABLE statement.
  */
case class OracleTableComment(tableNameParsed: Name, commentText: String)
    extends OracleStatement
    with TableCommentStatement:

  override def tableName: String =
    val parts = tableNameParsed.name.split("\\.")
    parts.last

  override def comment: String = commentText

// Type aliases for convenience
type OracleDropTableSchema = DropTable[OracleDropOptions]
type OracleCreateTable = CreateTable[OracleDataType, OracleColumnOptions, OracleTableOptions]
type OracleDropViewSchema = DropView[OracleDropViewOptions]
type OracleCreateIndexSchema = CreateIndex[OracleIndexOptions]

/** ALTER TABLE action type for Oracle parser.
  */
type ParserAlterTableAction = AlterTableAction[OracleDataType, OracleColumnOptions]

/** Column modification for ALTER TABLE MODIFY.
  */
case class ColumnModification(column: Name, dataType: Option[Name], annotations: Seq[ColumnAnnotation])

/** ALTER TABLE MODIFY action.
  */
case class AlterTableModify(modifications: Seq[ColumnModification]) extends ParserAlterTableAction

/** ALTER TABLE ADD action.
  */
case class AlterTableAdd(columns: Seq[ColumnDefinition], annotations: Seq[TableAnnotation] = Seq())
    extends ParserAlterTableAction

// Type alias for parser-level AlterTable statements
type OracleAlterTableSchema =
  AlterTable[OracleDataType, OracleColumnOptions, OracleTableOptions, ParserAlterTableAction]

object OracleAlterTableAction:
  def renameColumn(oldName: Name, newName: Name): ParserAlterTableAction =
    RenameColumn[OracleDataType, OracleColumnOptions](oldName.name, newName.name)

  def dropColumn(column: Name, cascade: Boolean = false): ParserAlterTableAction =
    DropColumn[OracleDataType, OracleColumnOptions](column.name, cascade)

  def dropColumns(columns: Seq[Name]): ParserAlterTableAction =
    DropColumns[OracleDataType, OracleColumnOptions](columns.map(_.name))

  def dropConstraint(constraint: Name, cascade: Boolean = false): ParserAlterTableAction =
    DropConstraint[OracleDataType, OracleColumnOptions](constraint.name, cascade)

  def addConstraint(constraintName: Name, constraint: TableConstraint): ParserAlterTableAction =
    AddNamedConstraint[OracleDataType, OracleColumnOptions](constraintName.name, constraint)
