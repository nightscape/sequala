package sequala.schema

object GenericSqlRenderer extends BaseSqlRenderers:

  given SqlRenderer[CommonDataType] with
    def toSql(dt: CommonDataType)(using config: SqlFormatConfig): String = dt match
      case VarChar(length) => s"VARCHAR($length)"
      case SqlChar(length) => s"CHAR($length)"
      case SqlInteger => "INTEGER"
      case SqlBigInt => "BIGINT"
      case SmallInt => "SMALLINT"
      case Decimal(p, s) => s"DECIMAL($p, $s)"
      case Numeric(p, s) => s"NUMERIC($p, $s)"
      case Real => "REAL"
      case DoublePrecision => "DOUBLE PRECISION"
      case SqlBoolean => "BOOLEAN"
      case SqlDate => "DATE"
      case SqlTime(prec, withTz) =>
        val precStr = prec.map(p => s"($p)").getOrElse("")
        val tzStr = if withTz then " WITH TIME ZONE" else ""
        s"TIME$precStr$tzStr"
      case SqlTimestamp(prec, withTz) =>
        val precStr = prec.map(p => s"($p)").getOrElse("")
        val tzStr = if withTz then " WITH TIME ZONE" else ""
        s"TIMESTAMP$precStr$tzStr"
      case SqlText => "TEXT"
      case SqlBlob => "BLOB"
      case SqlClob => "CLOB"

  given SqlRenderer[DropTable] with
    def toSql(dt: DropTable)(using config: SqlFormatConfig): String =
      val ifExistsStr = if dt.ifExists then "IF EXISTS " else ""
      val cascadeStr = if dt.cascade then " CASCADE" else ""
      s"DROP TABLE $ifExistsStr${quote(dt.tableName)}$cascadeStr"
