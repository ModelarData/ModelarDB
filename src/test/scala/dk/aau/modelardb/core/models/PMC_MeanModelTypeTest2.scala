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
package dk.aau.modelardb.core.models

import dk.aau.modelardb.core.DataPoint
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util


class PMC_MeanModelTypeTest2 extends AnyFunSuite with Matchers {

    test("Initialize") {
        val d1 = new DataPoint(1, 123L, 13.0f)
        val d2 = new DataPoint(2, 124L, 12.0f)
        val d3 = new DataPoint(3, 125L, 14.0f)
        val array = Array(d1, d2, d3)
        val list = util.Arrays.asList(array)
        val model = new PMC_MeanModelType(12, 10, 10)
        model.initialize(list)
        model.length() should equal (1)
    }
}
