package sequala.migrate

import sequala.schema.*
import sequala.schema.ast.{LineComment, SqlComment}

object SchemaDiffer:

  /** Normalize name for comparison based on case sensitivity setting */
  private def normalizeForComparison(name: String, caseInsensitive: Boolean): String =
    if caseInsensitive then name.toUpperCase else name

  /** Get normalized qualified name for comparison, treating defaultSchema as equivalent to None */
  private def normalizedQualifiedName[DT, CO <: ColumnOptions, TO <: TableOptions](
    t: Table[DT, CO, TO],
    options: DiffOptions
  ): String =
    val effectiveSchema = t.schema.filterNot(s => options.defaultSchema.contains(s))
    val qualName = effectiveSchema.fold(t.name)(s => s"$s.${t.name}")
    normalizeForComparison(qualName, options.caseInsensitive)

  /** Create a Diffable instance for tables with the given options */
  private def tableDiffable[DT <: DataType, CO <: ColumnOptions, TO <: TableOptions](
    options: DiffOptions
  ): Diffable[String, Table[DT, CO, TO], SchemaDiffOp] =
    new Diffable[String, Table[DT, CO, TO], SchemaDiffOp]:
      def extractKey(t: Table[DT, CO, TO]): String =
        normalizedQualifiedName(t, options)

      def deleteOp(t: Table[DT, CO, TO]): Seq[SchemaDiffOp] =
        Seq(DropTable(t.qualifiedName))

      def createOp(t: Table[DT, CO, TO]): Seq[SchemaDiffOp] =
        val createTable = CreateTable(t)
        val createIndexes = t.indexes.flatMap { idx =>
          idx.name.map(name => CreateIndex(name, t.qualifiedName, idx.columns, idx.unique, false, idx.where))
        }
        Seq(createTable) ++ createIndexes

      def modifyOp(from: Table[DT, CO, TO], to: Table[DT, CO, TO]): Seq[SchemaDiffOp] =
        diffTable(from, to, options).toSeq ++ diffIndexes(from, to)

  def diff[DT <: DataType, CO <: ColumnOptions, TO <: TableOptions](
    from: Seq[Table[DT, CO, TO]],
    to: Seq[Table[DT, CO, TO]],
    options: DiffOptions = CommonDiffOptions.empty
  ): Seq[SchemaDiffOp] =
    given Diffable[String, Table[DT, CO, TO], SchemaDiffOp] = tableDiffable(options)
    val tableDiffs = Differ.diff(from, to)
    val commentChanges = diffComments(from, to, options)
    tableDiffs ++ commentChanges

  def diffTable[DT <: DataType, CO <: ColumnOptions, TO <: TableOptions](
    from: Table[DT, CO, TO],
    to: Table[DT, CO, TO],
    options: DiffOptions = CommonDiffOptions.empty
  ): Option[AlterTable[DT, CO, TO, AlterTableAction[DT, CO]]] =
    val actions = diffColumns(from, to, options) ++
      diffPrimaryKey(from, to, options) ++
      diffForeignKeys(from, to) ++
      diffUniques(from, to) ++
      diffChecks(from, to)

    if actions.isEmpty then None
    else Some(AlterTable(to.qualifiedName, actions))

  private def diffColumns[DT <: DataType, CO <: ColumnOptions, TO <: TableOptions](
    from: Table[DT, CO, TO],
    to: Table[DT, CO, TO],
    options: DiffOptions
  ): Seq[AlterTableAction[DT, CO]] =
    val normalize = (name: String) => normalizeForComparison(name, options.caseInsensitive)
    val fromByName = from.columns.map(c => normalize(c.name) -> c).toMap
    val toByName = to.columns.map(c => normalize(c.name) -> c).toMap

    // PK columns are implicitly NOT NULL - collect normalized PK column names
    val fromPkCols = from.primaryKey.map(_.columns.map(normalize).toSet).getOrElse(Set.empty)
    val toPkCols = to.primaryKey.map(_.columns.map(normalize).toSet).getOrElse(Set.empty)

    val dropped = from.columns.filterNot(c => toByName.contains(normalize(c.name))).map { c =>
      DropColumn[DT, CO](c.name)
    }

    val added = to.columns.filterNot(c => fromByName.contains(normalize(c.name))).map { c =>
      AddColumn(c)
    }

    val modified = for
      toCol <- to.columns
      fromCol <- fromByName.get(normalize(toCol.name))
      // Normalize nullable for PK columns (PK columns are always NOT NULL)
      fromNullable = if fromPkCols.contains(normalize(fromCol.name)) then false else fromCol.nullable
      toNullable = if toPkCols.contains(normalize(toCol.name)) then false else toCol.nullable
      normalizedFromCol = fromCol.copy(nullable = fromNullable)
      normalizedToCol = toCol.copy(nullable = toNullable)
      if columnChanged(normalizedFromCol, normalizedToCol, options)
    yield
      val diffComment = describeColumnChanges(normalizedFromCol, normalizedToCol, options)
      ModifyColumn(toCol, Seq(LineComment(diffComment)))

    dropped ++ added ++ modified

  private def describeColumnChanges[DT, CO <: ColumnOptions](
    from: Column[DT, CO],
    to: Column[DT, CO],
    options: DiffOptions
  ): String =
    val changes = scala.collection.mutable.ArrayBuffer[String]()

    val fromDt = from.dataType match
      case dt: DataType => options.normalizeDataType(dt)
      case other => other
    val toDt = to.dataType match
      case dt: DataType => options.normalizeDataType(dt)
      case other => other

    if fromDt != toDt then
      val fromStr = from.dataType match
        case dt: DataType => dt.toSql
        case other => other.toString
      val toStr = to.dataType match
        case dt: DataType => dt.toSql
        case other => other.toString
      changes += s"TYPE $fromStr -> $toStr"

    if from.nullable != to.nullable then
      val fromNull = if from.nullable then "NULL" else "NOT NULL"
      val toNull = if to.nullable then "NULL" else "NOT NULL"
      changes += s"$fromNull -> $toNull"

    if from.default != to.default then
      val fromDef = from.default.getOrElse("(none)")
      val toDef = to.default.getOrElse("(none)")
      changes += s"DEFAULT $fromDef -> $toDef"

    if from.options != to.options then changes += "OPTIONS changed"

    s"${to.name}: ${changes.mkString(", ")}"

  private def columnChanged[DT, CO <: ColumnOptions](
    from: Column[DT, CO],
    to: Column[DT, CO],
    options: DiffOptions
  ): Boolean =
    val fromDt = from.dataType match
      case dt: DataType => options.normalizeDataType(dt)
      case other => other
    val toDt = to.dataType match
      case dt: DataType => options.normalizeDataType(dt)
      case other => other
    fromDt != toDt ||
    from.nullable != to.nullable ||
    from.default != to.default ||
    from.options != to.options

  private def diffPrimaryKey[DT, CO <: ColumnOptions, TO <: TableOptions](
    from: Table[DT, CO, TO],
    to: Table[DT, CO, TO],
    options: DiffOptions
  ): Seq[AlterTableAction[DT, CO]] =
    val fromPkNorm = from.primaryKey.map(options.normalizePrimaryKey)
    val toPkNorm = to.primaryKey.map(options.normalizePrimaryKey)
    (fromPkNorm, toPkNorm) match
      case (None, None) => Seq.empty
      case (Some(pk), None) =>
        // Use original name for DROP, not normalized
        from.primaryKey.get.name match
          case Some(name) => Seq(DropConstraint(name))
          case None => Seq(DropConstraint("PRIMARY"))
      case (None, Some(pk)) =>
        Seq(AddConstraint(PrimaryKeyConstraint(to.primaryKey.get)))
      case (Some(fromPk), Some(toPk)) if fromPk != toPk =>
        // Use original name for DROP, not normalized
        val dropName = from.primaryKey.get.name.getOrElse("PRIMARY")
        Seq(DropConstraint(dropName), AddConstraint(PrimaryKeyConstraint(to.primaryKey.get)))
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
  ): Seq[SchemaDiffOp] =
    val (fromNamed, fromUnnamed) = from.indexes.partition(_.name.isDefined)
    val (toNamed, toUnnamed) = to.indexes.partition(_.name.isDefined)

    val fromNamedMap = fromNamed.flatMap(i => i.name.map(_ -> i)).toMap
    val toNamedMap = toNamed.flatMap(i => i.name.map(_ -> i)).toMap

    val droppedNamed = fromNamed.filter(i => i.name.exists(!toNamedMap.contains(_))).flatMap { i =>
      i.name.map(name => DropIndex(name))
    }

    val addedNamed = toNamed.filter(i => i.name.exists(!fromNamedMap.contains(_))).flatMap { i =>
      i.name.map(name => CreateIndex(name, to.qualifiedName, i.columns, i.unique, false, i.where))
    }

    val modifiedNamed = for
      toIdx <- toNamed
      name <- toIdx.name
      fromIdx <- fromNamedMap.get(name)
      if indexChanged(fromIdx, toIdx)
    yield Seq(DropIndex(name), CreateIndex(name, to.qualifiedName, toIdx.columns, toIdx.unique, false, toIdx.where))

    val indexSignature = (i: Index) => (i.columns, i.unique)
    val fromUnnamedSigs = fromUnnamed.map(indexSignature).toSet
    val toUnnamedSigs = toUnnamed.map(indexSignature).toSet

    val addedUnnamed = toUnnamed.filter(i => !fromUnnamedSigs.contains(indexSignature(i))).flatMap { i =>
      i.name.orElse(Some(generateIndexName(to.qualifiedName, i))).map { name =>
        CreateIndex(name, to.qualifiedName, i.columns, i.unique, false, i.where)
      }
    }

    val droppedUnnamed = fromUnnamed.filter(i => !toUnnamedSigs.contains(indexSignature(i))).flatMap { i =>
      i.name.orElse(Some(generateIndexName(from.qualifiedName, i))).map { name =>
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

  private def diffComments[DT <: DataType, CO <: ColumnOptions, TO <: TableOptions](
    from: Seq[Table[DT, CO, TO]],
    to: Seq[Table[DT, CO, TO]],
    options: DiffOptions
  ): Seq[SchemaDiffOp] =
    val normalize = (t: Table[DT, CO, TO]) => normalizedQualifiedName(t, options)
    val fromByName = from.map(t => normalize(t) -> t).toMap

    val results = for
      toTable <- to
      fromTable <- fromByName.get(normalize(toTable)).toSeq
      diff <- diffTableComment(fromTable, toTable).toSeq ++ diffColumnComments(
        fromTable,
        toTable,
        options.caseInsensitive
      )
    yield diff

    results

  private def diffTableComment[DT, CO <: ColumnOptions, TO <: TableOptions](
    from: Table[DT, CO, TO],
    to: Table[DT, CO, TO]
  ): Option[SetTableComment] =
    if from.comment != to.comment then Some(SetTableComment(to.qualifiedName, to.comment))
    else None

  private def diffColumnComments[DT, CO <: ColumnOptions, TO <: TableOptions](
    from: Table[DT, CO, TO],
    to: Table[DT, CO, TO],
    caseInsensitive: Boolean = false
  ): Seq[SetColumnComment] =
    val normalize = (name: String) => normalizeForComparison(name, caseInsensitive)
    val fromByName = from.columns.map(c => normalize(c.name) -> c).toMap
    for
      toCol <- to.columns
      fromCol <- fromByName.get(normalize(toCol.name))
      if fromCol.comment != toCol.comment
    yield SetColumnComment(to.qualifiedName, toCol.name, toCol.comment)
