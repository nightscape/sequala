package sequala

import org.specs2.mutable.*
import org.specs2.specification.*

import fastparse.{parse, Parsed}

import sequala.common.parser.{ErrorMessage, SQL}
import sequala.ansi.ANSISQL

class ErrorMessageSpec extends Specification {
  "The error message formatter" >> {

    "Format SELECT typos reasonably" >> {
      val query = "SEALECT foo \nFROM bar;"
      val result = parse(query, ANSISQL.instance.select(_), verboseFailures = true)

      result must beAnInstanceOf[Parsed.Failure]
      ErrorMessage.format(query, result.asInstanceOf[Parsed.Failure]) must beEqualTo(
        query.split("\n")(0) + "\n" +
          "^--- expected SELECT"
      )
    }

  }
}
