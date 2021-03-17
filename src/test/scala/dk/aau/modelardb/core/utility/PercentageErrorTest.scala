/* Copyright 2018-2020 Aalborg University
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

import org.scalatest.wordspec.AnyWordSpec

class PercentageErrorTest extends AnyWordSpec {
  "percentage error must handle 0, 0 without NaN" in {
    assert(Static.percentageError(0, 0) == 0.0)
  }

  "outside error must handle 0, 0 as false" in {
    assert( ! Static.outsidePercentageErrorBound(10, 0, 0))
  }

  "percentage error must handle A == R as 0.0" in {
    assert(Static.percentageError(37, 37) == 0.0)
  }

  "Outside error must handle A == R as false" in {
    assert( ! Static.outsidePercentageErrorBound(10, 37, 37))
  }

  "percentage error must handle +, 0 as positive infinity" in {
    assert(Static.percentageError(1, 0).isPosInfinity)
  }

  "outside error must handle +, 0 as true " in {
    assert(Static.outsidePercentageErrorBound(10, 1, 0))
  }

  "percentage error must handle 0, + as 100.0" in {
    assert(Static.percentageError(0, 1) == 100.0)
  }

  "outside error must handle 0, + as true " in {
    assert(Static.outsidePercentageErrorBound(10, 0, 1))
  }

  "percentage error must handle -, 0 as positive infinity" in {
    assert(Static.percentageError(-1, 0).isPosInfinity)
  }

  "outside error must handle -, 0 as true " in {
    assert(Static.outsidePercentageErrorBound(10, -1, 0))
  }

  "percentage error must handle 0, - as 100.0" in {
    assert(Static.percentageError(0, -1) == 100.0)
  }

  "outside error must handle 0, - as true " in {
    assert(Static.outsidePercentageErrorBound(10, 0, -1))
  }
}
