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

import akka.stream.scaladsl.SourceQueueWithComplete
import dk.aau.modelardb.arrow.ArrowUtil
import dk.aau.modelardb.config.ModelarConfig
import dk.aau.modelardb.core._
import dk.aau.modelardb.core.utility.{Static, ValueFunction}
import dk.aau.modelardb.engines.{EngineUtilities, QueryEngine}
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession, sources}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.Timestamp
import scala.collection.mutable
import collection.JavaConverters._

class Spark(config: ModelarConfig, sparkStorage: SparkStorage) extends QueryEngine {

  var sparkSession: SparkSession = _

  def start(queue: SourceQueueWithComplete[SegmentGroup]): Unit = ???

  /** Public Methods **/
  def start(): Unit = {
    //Creates the Spark Session, Spark Streaming Context, and initializes the companion object
    val (ss, ssc) = initialize()
    sparkSession = ss

    //Starts listening for and executes queries using the user-configured interface
//    Interface.start(configuration, sparkStorage, q => ArrowUtil.dfToArrow(ss.sql(q)))

    //Ensures that Spark does not terminate until ingestion is safely stopped
    if (ssc != null) {
      Static.info("ModelarDB: awaiting termination")
      ssc.awaitTermination()
      Spark.getCache.flush()
    }
//    ss.stop()
  }

  def stop(): Unit = {
    sparkSession.stop()
  }

  override def execute(query: String): VectorSchemaRoot = {
    ArrowUtil.dfToArrow(sparkSession.sql(query))
  }

  /** Private Methods **/
  private def initialize(): (SparkSession, StreamingContext) = {

    //Constructs the necessary Spark Conf and Spark Session Builder
    val engine = config.engine
    val master = if (engine == "spark") "local[*]" else engine
    val conf = new SparkConf()
      .set("spark.streaming.unpersist", "false")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssb = SparkSession.builder.master(master).config(conf)

    //Checks if the Storage instance provided has native Apache Spark integration
    val dimensions = config.dimensions
    val correlation = config.correlations
    val spark = sparkStorage.open(ssb, dimensions)

    //Initializes storage and Spark with any new time series that the system must ingest
    val derivedSources = config.derivedTimeSeries
    val ssc = if (config.ingestors == 0) {
      Partitioner.initializeTimeSeries(config, sparkStorage.getMaxTid)
      sparkStorage.storeMetadataAndInitializeCaches(
        Array.empty,
        derivedSources,
        dimensions,
        config.models)
      Spark.initialize(spark, config, sparkStorage, Range(0,0))
      null
    } else {
//      configuration.containsOrThrow("modelardb.spark.streaming")
      val newGid = sparkStorage.getMaxGid + 1
      val timeSeries = Partitioner.initializeTimeSeries(config, sparkStorage.getMaxTid)
      val timeSeriesGroups = Partitioner.groupTimeSeries(correlation, dimensions, timeSeries, sparkStorage.getMaxGid)
      sparkStorage.storeMetadataAndInitializeCaches(timeSeriesGroups, derivedSources, dimensions, config.models)
      Spark.initialize(spark, config, sparkStorage, Range(newGid, newGid + timeSeriesGroups.size))
      setupStream(spark, timeSeriesGroups)
    }

    //Creates Spark SQL tables that can be queried using SQL
    val vp = Spark.getViewProvider
    val segmentView = vp.option("type", "Segment").load()
    segmentView.createOrReplaceTempView("Segment")
    val dataPointView = vp.option("type", "DataPoint").load()
    dataPointView.createOrReplaceTempView("DataPoint")
    SparkUDAF.initialize(spark)
    EngineUtilities.initialize(dimensions)

    //Last, return the Spark interfaces so they can be controlled
    (spark, ssc)
  }

  private def setupStream(spark: SparkSession, timeSeriesGroups: Array[TimeSeriesGroup]): StreamingContext = {
    //Creates a receiver per ingestor with each receiving a working set created by Partitioner.partitionTimeSeries
    val ssc = new StreamingContext(spark.sparkContext, Seconds(config.sparkStreaming))
    val mtidCache = Spark.getSparkStorage.mtidCache.asJava
    val workingSets = Partitioner.partitionTimeSeries(config, timeSeriesGroups, mtidCache, config.ingestors)
    if (workingSets.length != config.ingestors) {
      throw new java.lang.RuntimeException("ModelarDB: the Spark engine did not receive a workings sets for each receiver")
    }

    val modelReceivers = workingSets.map(ws => new WorkingSetReceiver(ws))
    val streams = modelReceivers.map(ssc.receiverStream(_))
    val stream = ssc.union(streams.toSeq)

    //If querying and temporary segments are disabled, segments can be written directly to disk without being cached
    if ( config.interface.isEmpty && config.maxLatency <= 0) {
      stream.foreachRDD(Spark.getCache.write(_))
      Static.info("ModelarDB: Spark Streaming initialized in bulk-loading mode")
    } else {
      stream.foreachRDD(Spark.getCache.update(_))
      Static.info("ModelarDB: Spark Streaming initialized in online-analytics mode")
    }

    //The streaming context is started and the context is returned
    ssc.start()
    ssc
  }
}

object Spark {
  /** Instance Variables **/
  private var parallelism: Int = _
  private var cache: SparkCache = _
  private var viewProvider: DataFrameReader = _
  private var sparkStorage: SparkStorage = _
  private var broadcastedTimeSeriesTransformationCache: Broadcast[Array[ValueFunction]] = _
  private val storageSegmentGroupsSchema: StructType = StructType(Seq(
    StructField("gid", IntegerType, nullable = false),
    StructField("start_time", TimestampType, nullable = false),
    StructField("end_time", TimestampType, nullable = false),
    StructField("mtid", IntegerType, nullable = false),
    StructField("model", BinaryType, nullable = false),
    StructField("gaps", BinaryType, nullable = false)))

  /** Constructors **/
  def initialize(spark: SparkSession, config: ModelarConfig, sparkStorage: SparkStorage, newGids: Range): Unit = {
    this.parallelism = spark.sparkContext.defaultParallelism
    this.viewProvider = spark.read.format("dk.aau.modelardb.engines.spark.ViewProvider")
    this.sparkStorage = null
    this.broadcastedTimeSeriesTransformationCache = spark.sparkContext.broadcast(sparkStorage.timeSeriesTransformationCache)
    this.sparkStorage = sparkStorage
    this.cache = new SparkCache(spark, config.batchSize, newGids)
  }

  /** Public Methods **/
  def getCache: SparkCache = Spark.cache
  def getViewProvider: DataFrameReader = Spark.viewProvider
  def getSparkStorage: SparkStorage = Spark.sparkStorage
  def getBroadcastedTimeSeriesTransformationCache: Broadcast[Array[ValueFunction]] = Spark.broadcastedTimeSeriesTransformationCache
  def getStorageSegmentGroupsSchema: StructType = this.storageSegmentGroupsSchema
  def isDataSetSmall(rows: RDD[_]): Boolean = rows.partitions.length <= parallelism

  def applyFiltersToDataFrame(df: DataFrame, filters: Array[Filter]): DataFrame = {
    //All filters must be parsed as a set of conjunctions as Apache Spark SQL represents OR as a separate case class
    val predicates = mutable.ArrayBuffer[String]()
    for (filter: Filter <- filters) {
      filter match {
        //Predicate push-down for gid using SELECT * FROM segment with GID = ? and gid IN (..)
        case sources.EqualTo("gid", value: Int) => predicates.append(s"gid = $value")
        case sources.EqualNullSafe("gid", value: Int) => predicates.append(s"gid = $value")
        case sources.In("gid", values: Array[Any]) => values.map(_.asInstanceOf[Int]).mkString("GID IN (", ",", ")")

        //Predicate push-down for start_time using SELECT * FROM segment WHERE et <=> ?
        case sources.GreaterThan("start_time", value: Timestamp) => predicates.append(s"start_time > '$value'")
        case sources.GreaterThanOrEqual("start_time", value: Timestamp) => predicates.append(s"start_time >= '$value'")
        case sources.LessThan("start_time", value: Timestamp) => predicates.append(s"start_time < '$value'")
        case sources.LessThanOrEqual("start_time", value: Timestamp) => predicates.append(s"start_time <= '$value'")
        case sources.EqualTo("start_time", value: Timestamp) => predicates.append(s"start_time = '$value'")

        //Predicate push-down for end_time using SELECT * FROM segment WHERE et <=> ?
        case sources.GreaterThan("end_time", value: Timestamp) => predicates.append(s"end_time > '$value'")
        case sources.GreaterThanOrEqual("end_time", value: Timestamp) => predicates.append(s"end_time >= '$value'")
        case sources.LessThan("end_time", value: Timestamp) => predicates.append(s"end_time < '$value'")
        case sources.LessThanOrEqual("end_time", value: Timestamp) => predicates.append(s"end_time <= '$value'")
        case sources.EqualTo("end_time", value: Timestamp) => predicates.append(s"end_time = '$value'")

        //If a predicate is not supported the information is simply logged to inform the user
        case p => Static.warn("ModelarDB: unsupported predicate " + p, 120)
      }
    }
    val predicate = predicates.mkString(" AND ")
    Static.info(s"ModelarDB: constructed predicates ($predicate)", 120)
    if (predicate.isEmpty) {
      df
    } else {
      df.where(predicate)
    }
  }
}
