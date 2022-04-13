/* Copyright 2022 The ModelarDB Contributors
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
package dk.aau.modelardb.engines.spark

import dk.aau.modelardb.remote.ArrowResultSet

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{ArrowUtils, DataFrame}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.unsafe.types.UTF8String

import java.util

class SparkResultSet(df: DataFrame) extends ArrowResultSet {

  /** Public Methods **/
  def get(): VectorSchemaRoot = {
    this.vsr
  }

  def hasNext(): Boolean = {
    this.rows.hasNext
  }

  def fillNext(): Unit = {
    var count = 0
    this.writer.reset()
    this.vsr.setRowCount(this.DEFAULT_TARGET_BATCH_SIZE)
    while (this.rows.hasNext && count < this.DEFAULT_TARGET_BATCH_SIZE) {
      //The rows are converted locally due to the lack of Encoder[InternalRow]
      this.writer.write(InternalRow.fromSeq(this.rows.next().toSeq.map({
        case s: String => UTF8String.fromString(s) //ArrowWriter assumes strings are UTF8String
        case f => f
      })))
      count += 1
    }
    this.writer.finish()
  }

  def close(): Unit = {}

  /** Instance Variables **/
  private val rows = {
    //ArrowWriter assumes that timestamps are Long and stored as microseconds while Java/Scala uses milliseconds
    val timestampColumns = df.dtypes.filter(_._2 == "TimestampType").map(_._1)
    val dfWithLongTimestamps = timestampColumns.foldLeft(df)({ case (df, columnName) =>
      df.withColumn(columnName, df(columnName).cast("long") * 1000000) //ArrowWriter only supports microseconds
    })

    //The check removes Spark's warnings about the data already being cached if the same query is executed
    if ( ! dfWithLongTimestamps.storageLevel.useMemory) {
      dfWithLongTimestamps.persist()
    }
    dfWithLongTimestamps.toLocalIterator
  }
  private val vsr = {
    val schema = ArrowUtils.toArrowSchema(df.schema, util.TimeZone.getDefault.getID)
    val vsr = VectorSchemaRoot.create(schema, new RootAllocator())
    vsr.setRowCount(this.DEFAULT_TARGET_BATCH_SIZE)
    vsr
  }
  private val writer = ArrowWriter.create(this.vsr)
}