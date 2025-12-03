package sequala.migrate

import sequala.schema.*

trait MigrationGenerator[DT <: DataType, CO <: ColumnOptions, TO <: TableOptions]:
  def generate(changes: Seq[SchemaDiffOp]): Seq[MigrationStep[DT, CO, TO]] =
    changes.map(toMigrationStep)

  def toMigrationStep(change: SchemaDiffOp): MigrationStep[DT, CO, TO] =
    change match
      case ct: CreateTable[DT, CO, TO] @unchecked =>
        MigrationStep(
          statement = ct,
          reverse = Some(DropTable(ct.table.name)),
          comment = s"Create table ${ct.table.name}"
        )

      case dt: DropTable[?] =>
        MigrationStep(statement = dt, reverse = None, comment = s"Drop table ${dt.tableName}")

      case at: AlterTable[DT, CO, TO, AlterTableAction[DT, CO]] @unchecked =>
        MigrationStep(statement = at, reverse = reverseAlterTable(at), comment = describeAlterTable(at))

      case ci: CreateIndex[?] =>
        MigrationStep(
          statement = ci,
          reverse = Some(DropIndex(ci.name)),
          comment = s"Create index ${ci.name} on ${ci.tableName}"
        )

      case di: DropIndex =>
        MigrationStep(statement = di, reverse = None, comment = s"Drop index ${di.name}")

  private def reverseAlterTable(
    at: AlterTable[DT, CO, TO, AlterTableAction[DT, CO]]
  ): Option[AlterTable[DT, CO, TO, AlterTableAction[DT, CO]]] =
    val reversedActions = at.actions.flatMap(reverseAction)
    if reversedActions.size == at.actions.size then Some(AlterTable(at.tableName, reversedActions.reverse))
    else None

  private def reverseAction(action: AlterTableAction[DT, CO]): Option[AlterTableAction[DT, CO]] =
    action match
      case AddColumn(col, _) =>
        Some(DropColumn(col.name))
      case DropColumn(name, _, _) =>
        None
      case ModifyColumn(_, _) =>
        None
      case RenameColumn(oldName, newName, _) =>
        Some(RenameColumn(newName, oldName))
      case AddConstraint(constraint, _) =>
        constraintName(constraint).map(name => DropConstraint(name))
      case DropConstraint(name, _, _) =>
        None

  private def constraintName(constraint: TableConstraint): Option[String] =
    constraint match
      case PrimaryKeyConstraint(pk) => pk.name
      case ForeignKeyConstraint(fk) => fk.name
      case UniqueConstraint(u) => u.name
      case CheckConstraint(c) => c.name

  private def describeAlterTable(at: AlterTable[DT, CO, TO, AlterTableAction[DT, CO]]): String =
    val actionDescriptions = at.actions.map(describeAction)
    s"Alter table ${at.tableName}: ${actionDescriptions.mkString(", ")}"

  private def describeAction(action: AlterTableAction[DT, CO]): String =
    action match
      case AddColumn(col, _) => s"add column ${col.name}"
      case DropColumn(name, _, _) => s"drop column $name"
      case ModifyColumn(col, _) => s"modify column ${col.name}"
      case RenameColumn(oldName, newName, _) => s"rename column $oldName to $newName"
      case AddConstraint(constraint, _) =>
        constraint match
          case PrimaryKeyConstraint(_) => "add primary key"
          case ForeignKeyConstraint(fk) => s"add foreign key${fk.name.map(n => s" $n").getOrElse("")}"
          case UniqueConstraint(u) => s"add unique constraint${u.name.map(n => s" $n").getOrElse("")}"
          case CheckConstraint(c) => s"add check constraint${c.name.map(n => s" $n").getOrElse("")}"
      case DropConstraint(name, _, _) => s"drop constraint $name"
