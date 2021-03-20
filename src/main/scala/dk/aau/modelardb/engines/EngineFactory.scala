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
package dk.aau.modelardb.engines

import dk.aau.modelardb.core.models.Segment
import dk.aau.modelardb.core.utility.{Pair, SegmentFunction, ValueFunction}
import dk.aau.modelardb.core.{Configuration, Partitioner, SegmentGroup, Storage}
import dk.aau.modelardb.engines.hsqldb.HSQLDBStorage

import java.util

object EngineFactory {

  /** Public Methods **/
  def startEngine(configuration: Configuration, storage: Storage): Unit = {
    //Extracts the name of the system from the engine connection string
    configuration.getString("modelardb.engine").takeWhile(_ != ':') match {
      case "local" => startLocal(configuration, storage.asInstanceOf[HSQLDBStorage]) //HACK: HSQLDBStorage matches the old Storage class
      case "derby" => new dk.aau.modelardb.engines.derby.Derby(configuration, storage).start()
      case "h2" => new dk.aau.modelardb.engines.h2.H2(configuration, storage).start()
      case "hsqldb" => new dk.aau.modelardb.engines.hsqldb.HSQLDB(configuration, storage).start()
      case "spark" => new dk.aau.modelardb.engines.spark.Spark(configuration, storage).start()
      case _ =>
        throw new java.lang.UnsupportedOperationException("ModelarDB: unknown value for modelardb.engine in the config file")
    }
  }

  /** Private Methods **/
  //TODO: Replace local with one of the three RDBMSs for single node testing
  private def startLocal(configuration: Configuration, storage: HSQLDBStorage): Unit = { //HACK: HSQLDBStorage matches the old Storage class
    if ( ! configuration.contains("modelardb.ingestors")) {
      return
    }

    if (configuration.getInteger("modelardb.ingestors") != 1) {
      throw new java.lang.RuntimeException("ModelarDB: the local engine only supports using one ingestor")
    }

    //Creates a method that drops temporary segments and one that store finalized segments in batches
    val consumeTemporary = new SegmentFunction {
      override def emit(gid: Int, startTime: Long, endTime: Long, mid: Int, parameters: Array[Byte], gaps: Array[Byte]): Unit = ()
    }

    val batchSize = configuration.getInteger("modelardb.batch")
    var batchIndex = 0
    val batch = new Array[SegmentGroup](batchSize)
    val consumeFinalized = new SegmentFunction {
      override def emit(gid: Int, startTime: Long, endTime: Long, mid: Int, parameters: Array[Byte], gaps: Array[Byte]): Unit = {
        batch(batchIndex) = new SegmentGroup(gid, startTime, endTime, mid, parameters, gaps)
        batchIndex += 1
        if (batchIndex == batchSize) {
          storage.storeSegmentGroups(batch, batchIndex)
          batchIndex = 0
        }
      }
    }

    val isTerminated = new java.util.function.BooleanSupplier {
      override def getAsBoolean: Boolean = false
    }

    val dimensions = configuration.getDimensions
    storage.open(dimensions)

    val timeSeries = Partitioner.initializeTimeSeries(configuration, storage.getMaxSID)
    val timeSeriesGroups = Partitioner.groupTimeSeries(configuration, timeSeries, storage.getMaxGID)
    val derivedTimeSeries = configuration.get("modelardb.source.derived")(0)
      .asInstanceOf[util.HashMap[Integer, Array[Pair[String, ValueFunction]]]]
    val models = configuration.getModels
    storage.initialize(timeSeriesGroups, derivedTimeSeries, dimensions, models)

    val midCache = storage.midCache
    val workingSets = Partitioner.partitionTimeSeries(configuration, timeSeriesGroups, midCache, 1)

    if (workingSets.length != 1) {
      throw new java.lang.RuntimeException("ModelarDB: the local engine did not receive exactly one working set")
    }
    val workingSet = workingSets(0)
    println(workingSet)
    workingSet.process(consumeTemporary, consumeFinalized, isTerminated)
    storage.storeSegmentGroups(batch, batchIndex)
    workingSet.logger.printWorkingSetResult()

    //DEBUG: for debugging we print the number of data points returned from storage
    var segmentDebugCount = 0L
    val sgit = storage.getSegmentGroups
    while(sgit.hasNext) {
      val segments = sgit.next().toSegments(storage)
      for (segment: Segment <- segments) {
        segmentDebugCount += segment.grid().count()
      }
    }
    println(s"Gridded: $segmentDebugCount\n=========================================================")
  }
}