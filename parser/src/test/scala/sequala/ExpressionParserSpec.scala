package sequala

import org.specs2.mutable.Specification
import sequala.ansi.ANSISQL
import sequala.schema.ast.{Expression, Name, *}

class ExpressionParserSpec extends Specification {
  // Helper function to parse expressions using ANSISQL.instance
  def Parse(input: String): Expression =
    fastparse.parse(input, ANSISQL.instance.expression(_)) match {
      case fastparse.Parsed.Success(expr, _) => expr
      case f: fastparse.Parsed.Failure => throw new sequala.common.parser.ParseException(f)
    }

  "The Expression Parser" should {

    "Parse Comparisons" >> {
      Parse("1 > 2") should beEqualTo(Comparison(LongPrimitive(1), Comparison.Op.Gt, LongPrimitive(2)))
    }

    "Parse Arithmetic" >> {
      Parse("1+2") should beEqualTo(Arithmetic(LongPrimitive(1), Arithmetic.Op.Add, LongPrimitive(2)))
      Parse("1 + 2") should beEqualTo(Arithmetic(LongPrimitive(1), Arithmetic.Op.Add, LongPrimitive(2)))
    }

    "Respect Order of Operations" >> {
      Parse("1+2*3") should beEqualTo(
        Arithmetic(
          LongPrimitive(1),
          Arithmetic.Op.Add,
          Arithmetic(LongPrimitive(2), Arithmetic.Op.Mult, LongPrimitive(3))
        )
      )
      Parse("1+2 > 3") should beEqualTo(
        Comparison(
          Arithmetic(LongPrimitive(1), Arithmetic.Op.Add, LongPrimitive(2)),
          Comparison.Op.Gt,
          LongPrimitive(3)
        )
      )
      Parse("1+2 > 3 AND true") should beEqualTo(
        Arithmetic(
          Comparison(
            Arithmetic(LongPrimitive(1), Arithmetic.Op.Add, LongPrimitive(2)),
            Comparison.Op.Gt,
            LongPrimitive(3)
          ),
          Arithmetic.Op.And,
          BooleanPrimitive(true)
        )
      )
    }

    "Parse NULL tests" >> {
      Parse("1 IS NULL") should beEqualTo(IsNull(LongPrimitive(1)))
      Parse("1 IS NOT NULL") should beEqualTo(Not(IsNull(LongPrimitive(1))))
    }

    "Parse NULL literals" >> {
      Parse("NULL") should beEqualTo(NullPrimitive())
    }

    "Parse Variables" >> {
      Parse("A") should beEqualTo(Column(sequala.schema.ast.Name("A")))
      Parse("R.A") should beEqualTo(Column(sequala.schema.ast.Name("A"), Some(sequala.schema.ast.Name("R"))))
      Parse("`A`") should beEqualTo(Column(sequala.schema.ast.Name("A", true)))
      Parse("`R`.A") should beEqualTo(Column(sequala.schema.ast.Name("A"), Some(sequala.schema.ast.Name("R", true))))
      Parse("Foo.`Bar`") should beEqualTo(
        Column(sequala.schema.ast.Name("Bar", true), Some(sequala.schema.ast.Name("Foo")))
      )
      Parse("int") should beEqualTo(Column(sequala.schema.ast.Name("int")))
      Parse("_c0") should beEqualTo(Column(sequala.schema.ast.Name("_c0")))
    }

    "Parse Strings" >> {
      Parse("'Foo'") should beEqualTo(StringPrimitive("Foo"))
      Parse("'Foo''sball'") should beEqualTo(StringPrimitive("Foo'sball"))
      Parse("'(SEX = ''M'' ) OR ( SEX = ''F'')'") should beEqualTo(StringPrimitive("(SEX = 'M' ) OR ( SEX = 'F')"))
    }

    "Parse IN Expressions" >> {
      Parse("R IN ('1', '2', '3')") should beEqualTo(
        InExpression(Column(Name("R")), Left(Seq(StringPrimitive("1"), StringPrimitive("2"), StringPrimitive("3"))))
      )
      Parse("R.`_c1` IN ('1', '2', '3')") should beEqualTo(
        InExpression(
          Column(Name("_c1", true), Some(Name("R"))),
          Left(Seq(StringPrimitive("1"), StringPrimitive("2"), StringPrimitive("3")))
        )
      )
    }

    "Parse CASE Expressions" >> {
      Parse("CASE WHEN A > 1 THEN B ELSE C END") should beEqualTo(
        CaseWhenElse(
          None,
          Seq(
            Comparison(Column(sequala.schema.ast.Name("A")), Comparison.Op.Gt, LongPrimitive(1)) -> Column(Name("B"))
          ),
          Column(Name("C"))
        )
      )
      Parse("CASE A WHEN 1 THEN B WHEN 2 THEN C ELSE D END") should beEqualTo(
        CaseWhenElse(
          Some(Column(Name("A"))),
          Seq(LongPrimitive(1) -> Column(Name("B")), LongPrimitive(2) -> Column(Name("C"))),
          Column(Name("D"))
        )
      )
      Parse("IF A = 1 THEN B ELSE C END") should beEqualTo(
        CaseWhenElse(
          None,
          Seq(
            Comparison(Column(sequala.schema.ast.Name("A")), Comparison.Op.Eq, LongPrimitive(1)) -> Column(Name("B"))
          ),
          Column(Name("C"))
        )
      )
    }

    "Parse BETWEEN Expressions" >> {
      Parse("A BETWEEN 1 AND 2") should beEqualTo(
        Arithmetic(
          Comparison(Column(sequala.schema.ast.Name("A")), Comparison.Op.Gte, LongPrimitive(1)),
          Arithmetic.Op.And,
          Comparison(Column(sequala.schema.ast.Name("A")), Comparison.Op.Lte, LongPrimitive(2))
        )
      )
      Parse("A+2 BETWEEN 1 AND 2") should beEqualTo(
        Arithmetic(
          Comparison(
            Arithmetic(Column(sequala.schema.ast.Name("A")), Arithmetic.Op.Add, LongPrimitive(2)),
            Comparison.Op.Gte,
            LongPrimitive(1)
          ),
          Arithmetic.Op.And,
          Comparison(
            Arithmetic(Column(sequala.schema.ast.Name("A")), Arithmetic.Op.Add, LongPrimitive(2)),
            Comparison.Op.Lte,
            LongPrimitive(2)
          )
        )
      )
    }

  }
}
