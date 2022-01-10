package dk.aau.modelardb.arrow

import dk.aau.modelardb.H2Provider
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class SchemasTest extends AnyFlatSpec with should.Matchers with H2Provider {

  it should "map schema to JDBC types" in {
    withH2AndTestData { statement =>
      val rs = statement.executeQuery("select * from segment")
      var count = 0
      while (rs.next()) {
        count += 1
      }
      val root = ArrowUtil.jdbcToArrow(rs)

      count should equal (10)
    }


  }


}
