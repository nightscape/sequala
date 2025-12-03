package sequala.common.statement

import sequala.schema.{ColumnCommentStatement, EmptyStatement, Statement, TableCommentStatement, Unparseable}

/** Result of parsing a single statement with position information */
case class StatementParseResult(result: Either[Unparseable, Statement], startPos: Int, endPos: Int)
