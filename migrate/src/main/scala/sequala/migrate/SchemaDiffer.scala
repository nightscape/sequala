package sequala.migrate

import sequala.schema.*

type SchemaDiff[DT <: DataType, CO <: ColumnOptions, TO <: TableOptions] =
  CreateTable[DT, CO, TO] | DropTable | AlterTable[DT, CO, TO, AlterTableAction[DT, CO]] | CreateIndex | DropIndex

object SchemaDiffer:

  def diff[DT <: DataType, CO <: ColumnOptions, TO <: TableOptions](
    from: Seq[Table[DT, CO, TO]],
    to: Seq[Table[DT, CO, TO]]
  ): Seq[SchemaDiff[DT, CO, TO]] =
    val fromByName = from.map(t => t.name -> t).toMap
    val toByName = to.map(t => t.name -> t).toMap

    val dropped = from.filterNot(t => toByName.contains(t.name)).map { t =>
      DropTable(t.name)
    }

    val addedTables = to.filterNot(t => fromByName.contains(t.name))
    val added = addedTables.map(t => CreateTable(t))

    val addedTableIndexes = addedTables.flatMap { t =>
      t.indexes.flatMap { idx =>
        idx.name.map(name => CreateIndex(name, t.name, idx.columns, idx.unique, where = idx.where))
      }
    }

    val tableModifications = for
      toTable <- to
      fromTable <- fromByName.get(toTable.name)
    yield (diffTable(fromTable, toTable), diffIndexes(fromTable, toTable))

    val modified = tableModifications.flatMap(_._1)
    val indexChanges = tableModifications.flatMap(_._2)

    dropped ++ added ++ addedTableIndexes ++ modified ++ indexChanges

  def diffTable[DT <: DataType, CO <: ColumnOptions, TO <: TableOptions](
    from: Table[DT, CO, TO],
    to: Table[DT, CO, TO]
  ): Option[AlterTable[DT, CO, TO, AlterTableAction[DT, CO]]] =
    val actions = diffColumns(from, to) ++
      diffPrimaryKey(from, to) ++
      diffForeignKeys(from, to) ++
      diffUniques(from, to) ++
      diffChecks(from, to)

    if actions.isEmpty then None
    else Some(AlterTable(to.name, actions))

  private def diffColumns[DT <: DataType, CO <: ColumnOptions, TO <: TableOptions](
    from: Table[DT, CO, TO],
    to: Table[DT, CO, TO]
  ): Seq[AlterTableAction[DT, CO]] =
    val fromByName = from.columns.map(c => c.name -> c).toMap
    val toByName = to.columns.map(c => c.name -> c).toMap

    val dropped = from.columns.filterNot(c => toByName.contains(c.name)).map { c =>
      DropColumn[DT, CO](c.name)
    }

    val added = to.columns.filterNot(c => fromByName.contains(c.name)).map { c =>
      AddColumn(c)
    }

    val modified = for
      toCol <- to.columns
      fromCol <- fromByName.get(toCol.name)
      if columnChanged(fromCol, toCol)
    yield ModifyColumn(toCol)

    dropped ++ added ++ modified

  private def columnChanged[DT, CO <: ColumnOptions](from: Column[DT, CO], to: Column[DT, CO]): Boolean =
    from.dataType != to.dataType ||
      from.nullable != to.nullable ||
      from.default != to.default ||
      from.options != to.options

  private def diffPrimaryKey[DT, CO <: ColumnOptions, TO <: TableOptions](
    from: Table[DT, CO, TO],
    to: Table[DT, CO, TO]
  ): Seq[AlterTableAction[DT, CO]] =
    (from.primaryKey, to.primaryKey) match
      case (None, None) => Seq.empty
      case (Some(pk), None) =>
        pk.name match
          case Some(name) => Seq(DropConstraint(name))
          case None => Seq(DropConstraint("PRIMARY"))
      case (None, Some(pk)) =>
        Seq(AddConstraint(PrimaryKeyConstraint(pk)))
      case (Some(fromPk), Some(toPk)) if fromPk != toPk =>
        val dropName = fromPk.name.getOrElse("PRIMARY")
        Seq(DropConstraint(dropName), AddConstraint(PrimaryKeyConstraint(toPk)))
      case _ => Seq.empty

  private def diffForeignKeys[DT, CO <: ColumnOptions, TO <: TableOptions](
    from: Table[DT, CO, TO],
    to: Table[DT, CO, TO]
  ): Seq[AlterTableAction[DT, CO]] =
    diffNamedConstraints(
      from.foreignKeys,
      to.foreignKeys,
      (fk: ForeignKey) => fk.name,
      (fk: ForeignKey) => ForeignKeyConstraint(fk)
    )

  private def diffUniques[DT, CO <: ColumnOptions, TO <: TableOptions](
    from: Table[DT, CO, TO],
    to: Table[DT, CO, TO]
  ): Seq[AlterTableAction[DT, CO]] =
    diffNamedConstraints(from.uniques, to.uniques, (u: Unique) => u.name, (u: Unique) => UniqueConstraint(u))

  private def diffChecks[DT, CO <: ColumnOptions, TO <: TableOptions](
    from: Table[DT, CO, TO],
    to: Table[DT, CO, TO]
  ): Seq[AlterTableAction[DT, CO]] =
    diffNamedConstraints(from.checks, to.checks, (c: Check) => c.name, (c: Check) => CheckConstraint(c))

  private def diffNamedConstraints[DT, CO <: ColumnOptions, C](
    from: Seq[C],
    to: Seq[C],
    getName: C => Option[String],
    toConstraint: C => TableConstraint
  ): Seq[AlterTableAction[DT, CO]] =
    val fromByName = from.flatMap(c => getName(c).map(_ -> c)).toMap
    val toByName = to.flatMap(c => getName(c).map(_ -> c)).toMap

    val dropped = from.filter(c => getName(c).exists(!toByName.contains(_))).flatMap { c =>
      getName(c).map(name => DropConstraint[DT, CO](name))
    }

    val added = to.filter(c => getName(c).exists(!fromByName.contains(_))).map { c =>
      AddConstraint[DT, CO](toConstraint(c))
    }

    val modified = for
      toC <- to
      name <- getName(toC)
      fromC <- fromByName.get(name)
      if fromC != toC
    yield Seq(DropConstraint[DT, CO](name), AddConstraint[DT, CO](toConstraint(toC)))

    dropped ++ added ++ modified.flatten

  private def diffIndexes[DT, CO <: ColumnOptions, TO <: TableOptions](
    from: Table[DT, CO, TO],
    to: Table[DT, CO, TO]
  ): Seq[CreateIndex | DropIndex] =
    val (fromNamed, fromUnnamed) = from.indexes.partition(_.name.isDefined)
    val (toNamed, toUnnamed) = to.indexes.partition(_.name.isDefined)

    val fromNamedMap = fromNamed.flatMap(i => i.name.map(_ -> i)).toMap
    val toNamedMap = toNamed.flatMap(i => i.name.map(_ -> i)).toMap

    val droppedNamed = fromNamed.filter(i => i.name.exists(!toNamedMap.contains(_))).flatMap { i =>
      i.name.map(name => DropIndex(name))
    }

    val addedNamed = toNamed.filter(i => i.name.exists(!fromNamedMap.contains(_))).flatMap { i =>
      i.name.map(name => CreateIndex(name, to.name, i.columns, i.unique, where = i.where))
    }

    val modifiedNamed = for
      toIdx <- toNamed
      name <- toIdx.name
      fromIdx <- fromNamedMap.get(name)
      if indexChanged(fromIdx, toIdx)
    yield Seq(DropIndex(name), CreateIndex(name, to.name, toIdx.columns, toIdx.unique, where = toIdx.where))

    val indexSignature = (i: Index) => (i.columns, i.unique)
    val fromUnnamedSigs = fromUnnamed.map(indexSignature).toSet
    val toUnnamedSigs = toUnnamed.map(indexSignature).toSet

    val addedUnnamed = toUnnamed.filter(i => !fromUnnamedSigs.contains(indexSignature(i))).flatMap { i =>
      i.name.orElse(Some(generateIndexName(to.name, i))).map { name =>
        CreateIndex(name, to.name, i.columns, i.unique, where = i.where)
      }
    }

    val droppedUnnamed = fromUnnamed.filter(i => !toUnnamedSigs.contains(indexSignature(i))).flatMap { i =>
      i.name.orElse(Some(generateIndexName(from.name, i))).map { name =>
        DropIndex(name)
      }
    }

    droppedNamed ++ addedNamed ++ modifiedNamed.flatten ++ droppedUnnamed ++ addedUnnamed

  private def indexChanged(from: Index, to: Index): Boolean =
    from.columns != to.columns || from.unique != to.unique || from.where != to.where

  private def generateIndexName(tableName: String, index: Index): String =
    val prefix = if index.unique then "uix" else "ix"
    val columnPart = index.columns.map(_.name).mkString("_")
    s"${prefix}_${tableName}_$columnPart"
