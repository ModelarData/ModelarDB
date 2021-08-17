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

import dk.aau.modelardb.core.TimeSeriesGroup
import dk.aau.modelardb.core.timeseries.TimeSeriesORC
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector
import org.apache.orc.OrcFile
import org.scalatest.Assertions
import org.scalatest.Assertions.{cancel, pending}
import org.scalatest.exceptions.TestCanceledException

import java.io.File
import scala.collection.mutable

trait TimeSeriesGroupProvider {

  /** Instance Variables **/
  private val orcTestDataKey: String = "MODELARDB_TEST_DATA_ORC"
  private val isOrcTestDataFolderSet = sys.env.contains(orcTestDataKey)
  var samplingInterval: Int = -1

  def withTSGroup[A](fun: Array[TimeSeriesGroup] => A): A = {
    if (!isOrcTestDataFolderSet) {
      cancel(s"$orcTestDataKey environment variable not set!")
    }
    fun(newTimeSeriesGroups)
  }

  /** Public Methods  **/
  private def newTimeSeriesGroups: Array[TimeSeriesGroup] = {
    newTimeSeriesGroups(sys.env(orcTestDataKey))
  }

  /** Private Methods **/
  private def newTimeSeriesGroups(folderPath: String): Array[TimeSeriesGroup] = {
    val testDataFolder = new File(folderPath)
    val testDataFiles = testDataFolder.listFiles().filter(f => f.isFile && f.getName.endsWith(".orc"))
    testDataFiles.zipWithIndex.map(pair => newTimeSeriesGroup(pair._1, pair._2 + 1))
  }

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
}