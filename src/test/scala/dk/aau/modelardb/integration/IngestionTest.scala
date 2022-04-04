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

import dk.aau.modelardb.core.models.{ModelTypeFactory, Segment}
import dk.aau.modelardb.core.utility.Static
import dk.aau.modelardb.core.{DataPoint, TimeSeriesGroup, WorkingSet}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{OutputStream, PrintStream}
import java.nio.ByteBuffer
import scala.collection.JavaConverters._
import scala.collection.mutable

//Integration Test
class IngestionTest extends AnyFlatSpec with Matchers {
  //HACK: Disable stdout during testing as some of the tests are very noisy
  System.setOut(new PrintStream(OutputStream.nullOutputStream()))
  System.setErr(new PrintStream(OutputStream.nullOutputStream()))

  behavior of "ModelarDB"
  it should "be able to ingest time series without any error" in new TimeSeriesGroupProvider {
    //newTimeSeriesGroups and samplingInterval are provided by TimeSeriesGroupProvider
    val (ats, rts) = ingest(() => newTimeSeriesGroups,() => samplingInterval, 0.0F)
    while (ats.hasNext && rts.hasNext) {
      ats.next().value should equal(rts.next().value)
    }
  }

  it should "be able to ingest time series within an error bound" in new TimeSeriesGroupProvider {
    //newTimeSeriesGroups and samplingInterval are provided by TimeSeriesGroupProvider
    val (ats, rts) = ingest(() => newTimeSeriesGroups,() => samplingInterval, 10.0F)
    while (ats.hasNext && rts.hasNext) {
      Static.percentageError(ats.next().value, rts.next().value) should be <= 10.0
    }
  }

  /** Private Methods **/
  def ingest(newTimeSeriesGroups: () => Array[TimeSeriesGroup], samplingInterval: () => Int, errorBound: Float):
  (Iterator[DataPoint], Iterator[DataPoint]) = {
    //Initialize
    val mtn = Array("dk.aau.modelardb.core.models.PMC_MeanModelType",
      "dk.aau.modelardb.core.models.SwingFilterModelType", "dk.aau.modelardb.core.models.FacebookGorillaModelType")
    val mts = () => ModelTypeFactory.getModelTypes(mtn, Range(1, mtn.length + 1).toArray, errorBound, 50)
    val segments = mutable.ArrayBuffer[Segment]()
    val modelTypes = mts()

    //Ingest
    val offset = ByteBuffer.allocate(12).putInt(1).putInt(1).putInt(0).array()
    val workingSet = new WorkingSet(newTimeSeriesGroups(), 1 / 10, mtn,
      Range(1, mtn.length + 1).toArray, errorBound, 50, 0)
      workingSet.process((_: Int, _: Long, _: Long, _: Int, _: Array[Byte], _: Array[Byte]) => (),
        (gid: Int, startTime: Long, endTime: Long, mtid: Int, model: Array[Byte], gaps: Array[Byte]) => {
          segments.append(modelTypes(mtid - 1).get(gid, startTime, endTime, samplingInterval(), model, offset)) //HACK: gid == tid
        },
        () => false)

    //Verify
    val realTimeSeriesGroups = newTimeSeriesGroups()
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
