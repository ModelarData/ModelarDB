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

import dk.aau.modelardb.core._
import dk.aau.modelardb.core.utility.{Static, ValueFunction}
import dk.aau.modelardb.engines.{EngineUtilities, QueryEngine}
import dk.aau.modelardb.remote.QueryInterface

import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ArrowUtils, DataFrame, DataFrameReader, SparkSession, sources}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot

import java.sql.Timestamp
import java.util

import scala.collection.mutable

import collection.JavaConverters._

class Spark(configuration: Configuration, sparkStorage: SparkStorage) extends QueryEngine {
  /** Instance Variable **/
  var sparkSession: SparkSession = _

  /** Public Methods **/
  def start(): Unit = {
    //Creates the Spark Session, Spark Streaming Context, and initializes the companion object
    val (ss, ssc) = initialize()
    this.sparkSession = ss

    //Starts listening for and executes queries using the user-configured interface
    QueryInterface.start(configuration, this)

    //Ensures that Spark does not terminate until ingestion is safely stopped
    if (ssc != null) {
      Static.info("ModelarDB: awaiting termination")
      ssc.awaitTermination()
      Spark.getCache.flush()
    }
    ss.stop()
  }

  def executeToJSON(query: String): Array[String] = {
    this.sparkSession.sql(query).toJSON.collect()
  }

  def executeToArrow(query: String): VectorSchemaRoot = {
    //Converts the schema of the query result to Arrow
    val df = sparkSession.sql(query)
    val schema = ArrowUtils.toArrowSchema(df.schema, util.TimeZone.getDefault.getID)
    val vsr = VectorSchemaRoot.create(schema, new RootAllocator())
    val writer = ArrowWriter.create(vsr)
    var count = 0

    //ArrowWriter assumes that timestamps are Long and stored as microseconds while Java/Scala uses milliseconds
    val timestampColumns = df.dtypes.filter(_._2 == "TimestampType").map(_._1)
    val dfWithLongTimestamps = timestampColumns.foldLeft(df)({ case (df, columnName) =>
      df.withColumn(columnName, df(columnName).cast("long") * 1000000) //ArrowWriter only supports microseconds
    })

    //Converts the rows in the query result to Arrow
    dfWithLongTimestamps.collect().foreach(row => {
      //Converted locally due to no Encoder[InternalRow]
      writer.write(InternalRow.fromSeq(row.toSeq))
      count += 1
    })
    writer.finish()
    vsr.setRowCount(count)
    vsr
  }

  /** Private Methods **/
  private def initialize(): (SparkSession, StreamingContext) = {

    //Constructs the necessary Spark Conf and Spark Session Builder
    val engine = configuration.getString("modelardb.engine")
    val master = if (engine == "spark") "local[*]" else engine
    val conf = new SparkConf()
      .set("spark.streaming.unpersist", "false")
      .set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssb = SparkSession.builder.master(master).config(conf)

    //Checks if the Storage instance provided has native Apache Spark integration
    val dimensions = configuration.getDimensions
    val spark = sparkStorage.open(ssb, dimensions)

    //Initializes storage and Spark with any new time series that the system must ingest
    configuration.containsOrThrow("modelardb.batch_size")
    val ssc = if (configuration.getIngestors == 0) {
      if ( ! configuration.getDerivedTimeSeries.isEmpty) { //Initializes derived time series
        Partitioner.initializeTimeSeries(configuration, sparkStorage.getMaxTid)
      }
      sparkStorage.storeMetadataAndInitializeCaches(configuration, Array())
      Spark.initialize(spark, configuration, sparkStorage, Range(0,0))
      null
    } else {
      configuration.containsOrThrow("modelardb.spark.streaming")
      val newGid = sparkStorage.getMaxGid + 1
      val timeSeries = Partitioner.initializeTimeSeries(configuration, sparkStorage.getMaxTid)
      val timeSeriesGroups = Partitioner.groupTimeSeries(configuration, timeSeries, sparkStorage.getMaxGid)
      sparkStorage.storeMetadataAndInitializeCaches(configuration, timeSeriesGroups)
      Spark.initialize(spark, configuration, sparkStorage, Range(newGid, newGid + timeSeriesGroups.size))
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
    val ssc = new StreamingContext(spark.sparkContext, Seconds(configuration.getInteger("modelardb.spark.streaming")))
    val mtidCache = Spark.getSparkStorage.mtidCache.asJava
    val workingSets = Partitioner.partitionTimeSeries(configuration, timeSeriesGroups, mtidCache,
      configuration.getInteger("modelardb.ingestors"))
    if (workingSets.length != configuration.getInteger("modelardb.ingestors")) {
      throw new java.lang.RuntimeException("ModelarDB: the Spark engine did not receive a workings sets for each receiver")
    }

    val modelReceivers = workingSets.map(ws => new WorkingSetReceiver(ws))
    val streams = modelReceivers.map(ssc.receiverStream(_))
    val stream = ssc.union(streams.toSeq)

    //If querying and temporary segments are disabled, segments can be written directly to disk without being cached
    if ( ! configuration.contains("modelardb.interface") && ! configuration.contains("modelardb.latency")) {
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
  def initialize(spark: SparkSession, configuration: Configuration, sparkStorage: SparkStorage, newGids: Range): Unit = {
    this.parallelism = spark.sparkContext.defaultParallelism
    this.viewProvider = spark.read.format("dk.aau.modelardb.engines.spark.ViewProvider")
    this.sparkStorage = null
    this.broadcastedTimeSeriesTransformationCache = spark.sparkContext.broadcast(sparkStorage.timeSeriesTransformationCache)
    this.sparkStorage = sparkStorage
    this.cache = new SparkCache(spark, configuration.getInteger("modelardb.batch_size"), newGids)
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

        //Predicate push-down for start_time using SELECT * FROM segment WHERE start_time <=> ?
        case sources.GreaterThan("start_time", value: Timestamp) => predicates.append(s"start_time > '$value'")
        case sources.GreaterThanOrEqual("start_time", value: Timestamp) => predicates.append(s"start_time >= '$value'")
        case sources.LessThan("start_time", value: Timestamp) => predicates.append(s"start_time < '$value'")
        case sources.LessThanOrEqual("start_time", value: Timestamp) => predicates.append(s"start_time <= '$value'")
        case sources.EqualTo("start_time", value: Timestamp) => predicates.append(s"start_time = '$value'")

        //Predicate push-down for end_time using SELECT * FROM segment WHERE end_time <=> ?
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
