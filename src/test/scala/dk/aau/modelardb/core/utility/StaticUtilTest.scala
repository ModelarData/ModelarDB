/* Copyright 2021 The ModelarDB Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
