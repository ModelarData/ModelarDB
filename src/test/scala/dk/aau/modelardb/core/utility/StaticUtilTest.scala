package dk.aau.modelardb.core.utility

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StaticUtilTest extends AnyFlatSpec with Matchers {

  behavior of "percentageError"

  it should "calculate percent error in pct" in {
      Static.percentageError(2.0, 4.0) should equal (50.0)
    }

  it should "return 0.0 when inputs are zero" in {
    Static.percentageError(0.0, 0.0) should equal (0.0)
  }

  it should "handle negative numbers" in {
    Static.percentageError(-102.0, -100.0) should equal (2.0)
  }


  behavior of "outsidePercentageErrorBound"

  it should "should return true when error is larger than bound" in {
    Static.outsidePercentageErrorBound(10.0f, 2.0, 4.0) should be (true)
  }

  it should "should return false when error is smaller than bound" in {
    Static.outsidePercentageErrorBound(3.0f, 102.0, 100.0) should be (false)
  }

  it should "handle negative numbers" in {
    Static.outsidePercentageErrorBound(3.0f, -102.0, -100.0) should be (false)
  }
}
