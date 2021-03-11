package dk.aau.modelardb.engines

import java.sql.Connection
import java.util
import java.util.function.BooleanSupplier
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable
import scala.collection.JavaConverters._

import dk.aau.modelardb.core.utility.{SegmentFunction, Pair, Static, ValueFunction}
import dk.aau.modelardb.core.{Configuration, DataPoint, Dimensions, Partitioner, SegmentGroup, Storage, WorkingSet}

//TODO: Use parameters and Configuration uniformly. Maybe remove get() and pass the configuration object around?
//TODO: Implement a proper cache for segments retrieved from storage. Maybe store them as Gid, ST, ET intervals?
//TODO: Prevent the system from automatically terminating on its own while there are still time series to ingest.
//TODO: Merge the two updateTemporarySegment methods, or better have the RDBMSs extract each SegmentGenerators buffer.
//TODO: Merge the loggers from each thread before printing them to make them easier to read the results.
//TODO: Make the two gridding methods used by the SparkEngine generic enough that all engines can use them.
//TODO: Remove resolution from Segment View so RDBMSs can run UDAFs on the stored rows if they are also used for storage.
class RDBMSEngineUtilities(storage: Storage, models: Array[String], batchSize: Int) {

  /** Public methods **/
  def startIngestion(dimensions: Dimensions): Unit = {
    //Initialize Storage
    val configuration = Configuration.get()
    storage.open(dimensions)
    val timeSeries = Partitioner.initializeTimeSeries(configuration, storage.getMaxSID)
    val timeSeriesGroups = Partitioner.groupTimeSeries(configuration, timeSeries, storage.getMaxGID)
    val derivedTimeSeries = configuration.get("modelardb.source.derived")(0)
      .asInstanceOf[util.HashMap[Integer, Array[Pair[String, ValueFunction]]]]
    storage.initialize(timeSeriesGroups, derivedTimeSeries, dimensions, models)
    if (timeSeriesGroups.isEmpty) {
      //There are no time series to ingest
      return
    }

    val midCache = storage.getMidCache
    //TODO: What name should be used for this parameter so all ingestions can use it, modelardb.ingestors?
    val threads = configuration.getInteger("modelardb.spark.receivers")
    val workingSets = Partitioner.partitionTimeSeries(configuration, timeSeriesGroups, midCache, threads)

    //Start Ingestion
    if (workingSets.nonEmpty) {
      for (workingSet <- workingSets) {
        new Thread {
          override def run {
            ingest(workingSet)
          }
        }.start()
      }
    }
  }

  def getSegmentGroups(): Iterator[SegmentGroup] = {
    //The cache is copied to not block ingestion while the query is executed, we assume storage provides a snapshot
    this.cacheLock.readLock().lock()
    val cachedTemporarySegments = this.temporarySegments.values.flatMap(sg => sg).toArray
    val cachedFinalizedSegments = this.finalizedSegments.take(this.finalizedSegmentsIndex)
    val storedFinalizedSegments = storage.getSegmentGroups.asScala
    this.cacheLock.readLock().unlock()
    cachedTemporarySegments.iterator ++ cachedFinalizedSegments.iterator ++ storedFinalizedSegments
  }

  def getDataPoints(): Iterator[DataPoint] = {
    RDBMSEngineUtilities.getUtilities.getSegmentGroups.flatMap(sg =>
      sg.toSegments(RDBMSEngineUtilities.getStorage))
      .flatMap(segment => segment.grid().iterator().asScala)
  }

  def executeQuery(connection: Connection, query: String): Array[String] = {
    //Execute Query
    val stmt = connection.createStatement()
    stmt.execute(query)
    val rs = stmt.getResultSet
    val md = rs.getMetaData

    //Format Result
    val result = mutable.ArrayBuffer[String]()
    val line = new StringBuilder()
    val columnSeparators = md.getColumnCount
    while (rs.next()) {
      var columnIndex = 1
      line.append('{')
      while (columnIndex < columnSeparators) {
        addColumnToOutput(md.getColumnName(columnIndex), rs.getObject(columnIndex), ',', line)
        columnIndex += 1
      }
      addColumnToOutput(md.getColumnName(columnIndex), rs.getObject(columnIndex), '}', line)
      result.append(line.mkString)
      line.clear()
    }

    //Close and Return
    rs.close()
    stmt.close()
    result.toArray
  }

  /** Private Methods **/
  private def ingest(workingSet: WorkingSet): Unit = {
    //Creates a method that stores temporary segments in memory and finalized segments in batches to be written to disk
    val consumeTemporary = new SegmentFunction {
      override def emit(gid: Int, startTime: Long, endTime: Long, mid: Int, parameters: Array[Byte], gaps: Array[Byte]): Unit = {
        cacheLock.writeLock().lock()
        val newTemporarySegment = new SegmentGroup(gid, startTime, endTime, mid, parameters, gaps)
        val currentTemporarySegments = temporarySegments.getOrElse(gid, Array())
        temporarySegments(gid) = updateTemporarySegment(currentTemporarySegments, newTemporarySegment, true)
        cacheLock.writeLock().unlock()
      }
    }

    val consumeFinalized = new SegmentFunction {
      override def emit(gid: Int, startTime: Long, endTime: Long, mid: Int, parameters: Array[Byte], gaps: Array[Byte]): Unit = {
        cacheLock.writeLock().lock()
        finalizedSegments(finalizedSegmentsIndex) = new SegmentGroup(gid, startTime, endTime, mid, parameters, gaps)
        finalizedSegmentsIndex += 1
        if (finalizedSegmentsIndex == batchSize) {
          storage.storeSegmentGroups(finalizedSegments, finalizedSegmentsIndex)
          finalizedSegmentsIndex = 0
        }
        cacheLock.writeLock().unlock()
      }
    }

    val isTerminated = new BooleanSupplier {
      override def getAsBoolean: Boolean = false
    }

    //Start Ingestion
    println(workingSet)
    workingSet.process(consumeTemporary, consumeFinalized, isTerminated)

    //Write remaining finalized segments
    cacheLock.writeLock().lock()
    storage.storeSegmentGroups(finalizedSegments, finalizedSegmentsIndex)
    finalizedSegmentsIndex = 0
    cacheLock.writeLock().unlock()
    workingSet.logger.printWorkingSetResult()
  }

  private def addColumnToOutput(columnName: String, value: AnyRef, end: Char, output: StringBuilder): Unit = {
    output.append('"')
    output.append(columnName)
    output.append('"')
    output.append(':')

    //Numbers should not be quoted
    if (value.isInstanceOf[Int] || value.isInstanceOf[Float]) {
      output.append(value)
    } else {
      output.append('"')
      output.append(value)
      output.append('"')
    }
    output.append(end)
  }

  private def updateTemporarySegment(cache: Array[SegmentGroup], inputSG: SegmentGroup, isTemporary: Boolean): Array[SegmentGroup]= {
    //The gaps are extracted from the new finalized or temporary segment
    val inputGaps = Static.bytesToInts(inputSG.offsets)

    //Extracts the metadata for the group of time series being updated
    val groupMetadataCache = storage.getGroupMetadataCache
    val group = groupMetadataCache(inputSG.gid).drop(1)
    val resolution = groupMetadataCache(inputSG.gid)(0)
    val inputIngested = group.toSet.diff(inputGaps.toSet)
    var updatedExistingSegment = false

    for (i <- cache.indices) {
      //The gaps are extracted for each existing temporary row
      val cachedSG = cache(i)
      val cachedGap = Static.bytesToInts(cachedSG.offsets)
      val cachedIngested = group.toSet.diff(cachedGap.toSet)

      //Each existing temporary segment that contains values for the same time series as the new segment is updated
      if (cachedIngested.intersect(inputIngested).nonEmpty) {
        if (isTemporary) {
          //A new temporary segment always represent newer data points than the previous temporary segment
          cache(i) = inputSG
        } else {
          //Moves the start time of the temporary segment to the data point right after the finalized segment, if
          // the new start time is after the end time of the temporary segment it can be dropped from the cache
          cache(i) = null //The current temporary segment is deleted if it overlaps completely with the finalized segment
          val startTime = inputSG.endTime + resolution
          if (startTime <= cachedSG.endTime) {
            val newGaps = Static.intToBytes(cachedGap :+ -((startTime - cachedSG.startTime) / resolution).toInt)
            cache(i) = new SegmentGroup(cachedSG.gid, startTime, cachedSG.endTime, cachedSG.mid, cachedSG.parameters, newGaps)
          }
        }
        updatedExistingSegment = true
      }
    }

    if (isTemporary && ! updatedExistingSegment) {
      //A split has occurred and multiple segments now represent what one did before, so the new ones are appended
      cache.filter(_ != null) :+ inputSG
    } else {
      //A join have occurred and one segment now represent what two did before, so duplicates must be removed
      cache.filter(_ != null).distinct
    }
  }

  /** Instance Variables **/
  private var finalizedSegmentsIndex = 0
  private val finalizedSegments: Array[SegmentGroup] = new Array[SegmentGroup](batchSize)
  private val cacheLock = new ReentrantReadWriteLock()
  private val temporarySegments = mutable.HashMap[Int, Array[SegmentGroup]]()
}

object RDBMSEngineUtilities {

  /** Public Methods **/
  def initialize(storage: Storage, models: Array[String], batchSize: Int): Unit = {
    RDBMSEngineUtilities.storage = storage
    RDBMSEngineUtilities.utilities = new RDBMSEngineUtilities(storage, models, batchSize)
  }

  def getStorage: Storage = RDBMSEngineUtilities.storage
  def getUtilities: RDBMSEngineUtilities = RDBMSEngineUtilities.utilities

  /** Instance Variables **/
  private var storage: Storage = _
  private var utilities: RDBMSEngineUtilities = _
}