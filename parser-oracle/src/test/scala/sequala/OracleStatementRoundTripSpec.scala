package sequala

import org.scalacheck.Gen
import sequala.oracle.{OracleSQL, OracleStatement}
import sequala.schema.oracle.{
  Bytes,
  Chars,
  Number,
  OracleChar,
  OracleColumnOptions,
  OracleCompressBasic,
  OracleCompressType,
  OracleCreateViewOptions,
  OracleDataType,
  OracleDialect,
  OracleDropOptions,
  OracleDropViewOptions,
  OracleExplainOptions,
  OracleParallelClause,
  OracleStorageClause,
  OracleTableOptions,
  SizeSemantics,
  Varchar2
}
import sequala.schema.{CommonDataType, CreateTable, Real, SqlFormatConfig, SqlRenderer, Statement}
import fastparse.Parsed

object OracleStatementRoundTripSpec extends OracleStatementRoundTripSpec

class OracleStatementRoundTripSpec extends StatementRoundTripSpec("Oracle statement round-trip") with OracleDialect {

  // Override to use Oracle parser
  override def parseStatement(sql: String): Parsed[Statement] = {
    // OracleSQL.apply parses a single statement
    OracleSQL(sql)
  }

  // Override to provide Oracle statement renderer
  override def statementRenderer(using config: SqlFormatConfig): SqlRenderer[Statement] = {
    import sequala.schema.oracle.OracleSqlRenderer.given_SqlRenderer_CreateOracleTable as createTableRenderer
    import sequala.schema.oracle.OracleSqlRenderer.given

    // Create a renderer for Statement that handles CreateTable
    new SqlRenderer[Statement] {
      def toSql(stmt: Statement)(using config: SqlFormatConfig): String = stmt match {
        case ct: CreateTable[?, ?, ?] =>
          // Cast to Oracle types - this is safe because we only generate Oracle types
          createTableRenderer.toSql(
            ct.asInstanceOf[CreateTable[OracleDataType, OracleColumnOptions, OracleTableOptions]]
          )
        case _ => throw new IllegalArgumentException(s"Unsupported statement type: ${stmt.getClass}")
      }
    }
  }

  // Override data type generator for Oracle types
  // Use Oracle-native types for proper round-tripping
  override def genSchemaDataType: Gen[DataType] = {
    Gen.oneOf(
      // Oracle-specific data types (prefer these for round-trip consistency)
      Gen.choose(1, 4000).map(n => Varchar2(n, Bytes)),
      Gen.choose(1, 4000).map(n => Varchar2(n, Chars)),
      Gen.choose(1, 2000).map(n => OracleChar(n, Bytes)),
      Gen.const(Number(None, None)),
      Gen.choose(1, 38).map(p => Number(Some(p), None)),
      Gen.choose(1, 38).flatMap(p => Gen.choose(0, p).map(s => Number(Some(p), Some(s)))),
      Gen.const(sequala.schema.oracle.BinaryFloat),
      Gen.const(sequala.schema.oracle.BinaryDouble)
    )
  }

  // Normalizes a data type to what it becomes after Oracle SQL round-trip.
  // Real and DoublePrecision map to Oracle-specific types after parsing.
  def normalizeDataType(dt: DataType): DataType = dt match {
    // Common types that map to Oracle-specific types after round-trip
    case Real => sequala.schema.oracle.BinaryFloat
    case sequala.schema.DoublePrecision => sequala.schema.oracle.BinaryDouble
    // Everything else stays the same (NUMBER types now round-trip as Number)
    case other => other
  }

  // Override column options for Oracle virtual/invisible columns
  override def genColumnOptions: Gen[ColumnOptions] = {
    Gen.oneOf(
      Gen.const(OracleColumnOptions.empty),
      Gen.const(OracleColumnOptions(invisible = true))
      // Note: virtual columns with expressions are complex and may not round-trip cleanly
      // so we omit them for now
    )
  }

  // Override table options for Oracle tablespace/storage/etc
  override def genTableOptions: Gen[TableOptions] = {
    for {
      tablespace <- Gen.option(Gen.oneOf("USERS", "SYSTEM", "TABLESPACE_1"))
      pctFree <- Gen.option(Gen.chooseNum(0, 50))
      pctUsed <- Gen.option(Gen.chooseNum(0, 99))
      storage <- Gen.option(genOracleStorageClause)
      logging <- Gen.option(Gen.oneOf(true, false))
      compress <- Gen.option(genOracleCompressType)
      cache <- Gen.option(Gen.oneOf(true, false))
      parallel <- Gen.option(genOracleParallelClause)
      rowMovement <- Gen.option(Gen.oneOf(true, false))
    } yield OracleTableOptions(
      tablespace = tablespace,
      pctFree = pctFree,
      pctUsed = pctUsed,
      iniTrans = None, // Omit for simplicity
      maxTrans = None, // Omit for simplicity
      storage = storage,
      logging = logging,
      compress = compress,
      cache = cache,
      parallel = parallel,
      rowMovement = rowMovement
    )
  }

  // Oracle-specific storage clause generator
  def genOracleStorageClause: Gen[OracleStorageClause] = {
    for {
      initial <- Gen.option(Gen.oneOf("64K", "128K", "1M"))
      next <- Gen.option(Gen.oneOf("64K", "128K"))
      minExtents <- Gen.option(Gen.chooseNum(1, 10))
      maxExtents <- Gen.option(Gen.oneOf("UNLIMITED", "100", "1000"))
      pctIncrease <- Gen.option(Gen.chooseNum(0, 100))
      bufferPool <- Gen.option(Gen.oneOf("DEFAULT", "KEEP", "RECYCLE"))
    } yield OracleStorageClause(
      initial = initial,
      next = next,
      minExtents = minExtents,
      maxExtents = maxExtents,
      pctIncrease = pctIncrease,
      bufferPool = bufferPool
    )
  }

  // Oracle-specific compress type generator
  def genOracleCompressType: Gen[OracleCompressType] = {
    Gen.const(OracleCompressBasic)
    // Note: OracleCompressAdvanced and OracleCompressFor are more complex
    // and may require additional parsing support
  }

  // Oracle-specific parallel clause generator
  def genOracleParallelClause: Gen[OracleParallelClause] = {
    Gen.option(Gen.chooseNum(1, 16)).map(degree => OracleParallelClause(degree))
  }

  // Override genStatement to only generate CREATE TABLE statements for now
  // This focuses the test on the table options round-trip issue
  override val genStatement: Gen[Statement] = genCreateTableStatement

  // Override drop options generator
  override def genDropOptions: Gen[DropOptions] =
    Gen.const(OracleDropOptions.empty)

  // Override create view options generator
  override def genCreateViewOptions: Gen[CreateViewOptions] =
    Gen.const(OracleCreateViewOptions.empty)

  // Override drop view options generator
  override def genDropViewOptions: Gen[DropViewOptions] =
    Gen.const(OracleDropViewOptions.empty)

  // Override explain options generator
  override def genExplainOptions: Gen[ExplainOptions] =
    Gen.const(OracleExplainOptions.empty)

  // Override statementsEqual to properly compare Oracle table options
  // Uses normalizeDataType to account for type mappings during round-trip
  override def statementsEqual(original: Statement, parsed: Statement): Boolean = (original, parsed) match {
    case (o: CreateGenericTable, p: CreateGenericTable) =>
      // Normalize original data types before comparison
      val normalizedOriginalColumns =
        o.table.columns.map(c => (c.name, c.nullable, normalizeDataType(c.dataType), c.options))
      val parsedColumns = p.table.columns.map(c => (c.name, c.nullable, c.dataType, c.options))

      val columnsEqual = normalizedOriginalColumns == parsedColumns
      val basicEqual = o.table.name == p.table.name && columnsEqual && o.orReplace == p.orReplace

      // Also compare table options!
      val optionsEqual = o.table.options == p.table.options

      if !columnsEqual then {
        println(s"Columns mismatch!")
        println(s"Original columns (normalized): $normalizedOriginalColumns")
        println(s"Parsed columns: $parsedColumns")
      }

      if !optionsEqual then {
        println(s"Table options mismatch!")
        println(s"Original options: ${o.table.options}")
        println(s"Parsed options: ${p.table.options}")
      }

      basicEqual && optionsEqual
    case _ => original == parsed
  }
}
