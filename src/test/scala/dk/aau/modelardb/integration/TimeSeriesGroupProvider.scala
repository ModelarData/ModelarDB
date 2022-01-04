/* Copyright 2018 The ModelarDB Contributors
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

import dk.aau.modelardb.core.utility.SegmentFunction;
import dk.aau.modelardb.core.timeseries.TimeSeriesORC
import dk.aau.modelardb.core.{SegmentGroup, TimeSeriesGroup, WorkingSet}
import dk.aau.modelardb.core.models.{ModelTypeFactory, Segment}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector
import org.apache.orc.OrcFile

import org.scalatest.Assertions

import java.io.File
import java.nio.ByteBuffer
import scala.collection.mutable

trait TimeSeriesGroupProvider extends Assertions {

  /** Instance Variables **/
  private var samplingInterval = -1  //HACK: assumes all test files uses the same sampling interval
  protected val modelTypeNames = Array(
    "dk.aau.modelardb.core.models.PMC_MeanModelType",
    "dk.aau.modelardb.core.models.SwingFilterModelType",
    "dk.aau.modelardb.core.models.FacebookGorillaModelType")
  assume(TimeSeriesGroupProvider.testDataWasProvided, TimeSeriesGroupProvider.missingTestDataMessage)

  /** Public Methods  **/
  def getTimeSeriesFiles: Array[File] = {
    new File(sys.env(TimeSeriesGroupProvider.orcTestDataKey))
      .listFiles().filter(f => f.isFile && f.getName.endsWith(".orc"))
  }

  def getSamplingInterval() = {
    if (this.samplingInterval == -1) {
      getTimeSeriesGroups
    }
    samplingInterval
  }

  def getTimeSeriesGroups: Array[TimeSeriesGroup] = {
    getTimeSeriesFiles.zipWithIndex.map(pair => newTimeSeriesGroup(pair._1, pair._2 + 1))
  }


  def ingestTimeSeriesGroupsToSegmentGroups(errorBound: Float): Array[SegmentGroup] = {
    val segments = mutable.ArrayBuffer[SegmentGroup]()
    val finalizedSegmentFunction = new SegmentFunction {
      override def emit(gid: Int, startTime: Long, endTime: Long, mtid: Int, model: Array[Byte], gaps: Array[Byte]): Unit = {
        segments.append(new SegmentGroup(gid, startTime, endTime, mtid, model, gaps))
      }
    }

    ingestTimeSeriesGroups(finalizedSegmentFunction, errorBound)
    segments.toArray
  }

  def ingestTimeSeriesGroupsToSegments(errorBound: Float): Array[Segment] = {
    val modelTypes = ModelTypeFactory.getModelTypes(modelTypeNames,
      Range(1, modelTypeNames.length + 1).toArray, errorBound, 50)
    val offset = ByteBuffer.allocate(12).putInt(1).putInt(1).putInt(0).array()
    val segments = ingestTimeSeriesGroupsToSegmentGroups(errorBound) //HACK: gid == tid
    segments.map(s => modelTypes(s.mtid - 1).get(s.gid, s.startTime, s.endTime, samplingInterval, s.model, offset))
  }

  /** Private Methods **/
  private def newTimeSeriesGroup(orcTestFile: File, id: Int): TimeSeriesGroup = {
    val orcTestFileAbsolutePath = orcTestFile.getAbsolutePath
    val path = new Path(orcTestFileAbsolutePath)
    val ro = OrcFile.readerOptions(new Configuration())
    val reader = OrcFile.createReader(path, ro)
    val schema = reader.getSchema

    //Infer timestampColumnIndex and valueColumnIndex
    var timestampColumnIndex = -1
    var valueColumnIndex = -1
    for (columnAndIndex <- schema.getFieldNames.toArray(Array[String]()).zipWithIndex) {
      val typeAsString = schema.findSubtype(columnAndIndex._1)
      if (typeAsString.toString.equals("timestamp")) {
        timestampColumnIndex = columnAndIndex._2
      }
      if (typeAsString.toString.equals("float")) {
        valueColumnIndex = columnAndIndex._2
      }
    }

    //Infer the samplingInterval
    val recordReader = reader.rows()
    val rowBatch = reader.getSchema.createRowBatch()
    recordReader.nextBatch(rowBatch)
    val counter = mutable.HashMap[Long, Int]()
    rowBatch.cols(timestampColumnIndex).asInstanceOf[TimestampColumnVector]
      .time.sliding(size = 2, step = 1).foreach(pair => {
        val elapsedTime = pair(1) - pair(0)
        counter.put(elapsedTime , counter.getOrElse(elapsedTime, 0) + 1)
      })
      this.samplingInterval = counter.maxBy(_._2)._1.toInt
      recordReader.close()

      //Instantiate the time series with the inferred values
      assert(this.samplingInterval > -1 && timestampColumnIndex > -1 && valueColumnIndex > -1)
      val tso = new TimeSeriesORC(orcTestFileAbsolutePath, id, this.samplingInterval, timestampColumnIndex, valueColumnIndex)
      new TimeSeriesGroup(id, Array(tso))
  }

  private def ingestTimeSeriesGroups(finalizedSegmentFunction: SegmentFunction, errorBound: Float): Unit = {
    val workingSet = new WorkingSet(getTimeSeriesGroups, 1 / 10, modelTypeNames,
      Range(1, modelTypeNames.length + 1).toArray, errorBound, 50, 0)

    workingSet.process((_: Int, _: Long, _: Long, _: Int, _: Array[Byte], _: Array[Byte]) => (),
      finalizedSegmentFunction, () => false)
  }
}

object TimeSeriesGroupProvider {
  val orcTestDataKey: String = "MODELARDB_TEST_DATA_ORC"
  val testDataWasProvided = sys.env.contains(orcTestDataKey)
  val missingTestDataMessage = (", set the environment variable "
    + orcTestDataKey + " to the path of Apache ORC files containing timestamps and values")
}
