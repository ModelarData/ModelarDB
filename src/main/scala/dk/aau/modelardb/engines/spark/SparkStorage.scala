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
package dk.aau.modelardb.engines.spark

import dk.aau.modelardb.core.Dimensions
import dk.aau.modelardb.storage.Storage
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{DataFrame, SparkSession}

trait SparkStorage extends Storage {
  def open(ssb: SparkSession.Builder, dimensions: Dimensions): SparkSession
  def storeSegmentGroups(sparkSession: SparkSession, df: DataFrame): Unit
  def getSegmentGroups(sparkSession: SparkSession, filters: Array[Filter]): DataFrame
  def storeTimeseries(tid: Int, scalingFactor: Float, samplingInterval: Int, gid: Int): Unit
}
