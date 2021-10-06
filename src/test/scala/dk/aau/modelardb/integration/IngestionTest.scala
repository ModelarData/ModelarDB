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
package dk.aau.modelardb.integration

import dk.aau.modelardb.core.DataPoint
import dk.aau.modelardb.core.models.Segment
import dk.aau.modelardb.core.TimeSeriesGroup
import dk.aau.modelardb.core.utility.Static

import scala.collection.JavaConverters._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IngestionTest extends AnyFlatSpec with Matchers {

  "ModelarDB" should "be able to ingest time series without any error" in new TimeSeriesGroupProvider {
    //ingestTimeSeriesGroupsToSegment and getTimeSeriesGroups are provided by TimeSeriesGroupProvider
    val segments = ingestTimeSeriesGroupsToSegments(0)
    val (ats, rts) = grid(segments, getTimeSeriesGroups)
    while (ats.hasNext && rts.hasNext) { ats.next().value should equal(rts.next().value)
    }
  }

  it should "be able to ingest time series within an error bound" in new TimeSeriesGroupProvider {
    //ingestTimeSeriesGroupsToSegment and getTimeSeriesGroups are provided by TimeSeriesGroupProvider
    val segments = ingestTimeSeriesGroupsToSegments(10.0F)
    val (ats, rts) = grid(segments, getTimeSeriesGroups)
    while (ats.hasNext && rts.hasNext) {
      Static.percentageError(ats.next().value, rts.next().value) should be <= 10.0
    }
  }

  /** Private Methods **/
  def grid(segments: Array[Segment], realTimeSeriesGroups: Array[TimeSeriesGroup]):
  (Iterator[DataPoint], Iterator[DataPoint]) = {
    val rts = new Iterator[DataPoint] {
      private var currentTimeSeriesIndex = 0
      private var currentTimeSeries = realTimeSeriesGroups(currentTimeSeriesIndex).getTimeSeries()(0)
      currentTimeSeries.open()

      override def hasNext: Boolean = {
        if (currentTimeSeries.hasNext) {
          return true
        }
        currentTimeSeries.close()
        currentTimeSeriesIndex += 1
        if (currentTimeSeriesIndex < realTimeSeriesGroups.length) {
          currentTimeSeries = realTimeSeriesGroups(currentTimeSeriesIndex).getTimeSeries()(0)
          currentTimeSeries.open()
          currentTimeSeries.hasNext //Ensures the first set of vectors are read
          return true
        }
        false
      }

      override def next(): DataPoint = currentTimeSeries.next()
    }
    (segments.iterator.flatMap(s => s.grid().iterator.asScala), rts)
  }
}
