package dk.aau.modelardb.engines.h2

import java.sql.{Connection, DriverManager}
import dk.aau.modelardb.Interface
import dk.aau.modelardb.core.utility.{Pair, SegmentFunction, Static, ValueFunction}
import dk.aau.modelardb.core.{Configuration, Dimensions, Partitioner, SegmentGroup, WorkingSet}
import dk.aau.modelardb.engines.PredicatePushDown
import dk.aau.modelardb.core.Dimensions.Types
import org.h2.expression.condition.{Comparison, ConditionAndOr, ConditionInConstantSet}
import org.h2.expression.{Expression, ExpressionColumn, ValueExpression}
import org.h2.value.{ValueInt, ValueTimestamp}

import java.util
import java.util.TimeZone
import java.util.HashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.function.BooleanSupplier
import scala.collection.mutable

//TODO: Can a WAL on data points be implemented per ingestor if it of static size (time) and have a bitmap of groups?
//TODO: Determine if adding the values to a pre-allocated array in the views is faster than having the branches.
//TODO: Determine if the data point views should get segments from the segment views for filtering like spark
//TODO: Implement a proper cache for segments retrieved from storage. Maybe store them as Gid, ST, ET intervals.
//TODO: Merge the loggers from each thread before printing them to make them easier to read the results.
//TODO: Make the two gridding methods used by the SparkEngine generic enough that all engines can use them.
//TODO: Remove resolution from Segment View so RDBMSs as it is avaliable from the metadata cache in storage.
//TODO: Determine how to ingest and execute queries in parallel without ever introducing duplicate data points.
//      Maybe having a shared read/write lock on storage (instead of engine) that engines can use when reading or writing.
//TODO: Support requiredColumns and test that predicate push-down works with all storage backends.
//TODO: determine if the segments should be filtered by the segment view and than data point view like done for Spark
//TODO: Share as much code as possible between H2 and Spark and structure their use of the two views the same.
class H2(configuration: Configuration, h2storage: H2Storage) {
  /** Instance Variables **/
  private var finalizedSegmentsIndex = 0
  private val finalizedSegments: Array[SegmentGroup] = new Array[SegmentGroup](configuration.getInteger("modelardb.batch"))
  private var numberOfRunningIngestors: CountDownLatch = _
  private val cacheLock = new ReentrantReadWriteLock()
  private val temporarySegments = mutable.HashMap[Int, Array[SegmentGroup]]()

  /** Public Methods **/
  def start(): Unit = {
    //Initialize
    //Documentation: http://www.h2database.com/html/features.html#in_memory_databases
    val connection = DriverManager.getConnection("jdbc:h2:mem:")
    val stmt = connection.createStatement()
    //Documentation: https://www.h2database.com/html/commands.html#create_table
    stmt.execute(H2.getCreateDataPointViewSQL(configuration.getDimensions))
    stmt.execute(H2.getCreateSegmentViewSQL(configuration.getDimensions))
    //Documentation: https://www.h2database.com/html/commands.html#create_aggregate
    stmt.execute(H2.getCreateUDAFSQL("COUNT_S"))
    stmt.execute(H2.getCreateUDAFSQL("MIN_S"))
    stmt.execute(H2.getCreateUDAFSQL("MAX_S"))
    stmt.execute(H2.getCreateUDAFSQL("SUM_S"))
    stmt.execute(H2.getCreateUDAFSQL("AVG_S"))

    stmt.execute(H2.getCreateUDAFSQL("COUNT_MONTH"))
    stmt.execute(H2.getCreateUDAFSQL("MIN_MONTH"))
    stmt.execute(H2.getCreateUDAFSQL("MAX_MONTH"))
    stmt.execute(H2.getCreateUDAFSQL("SUM_MONTH"))
    stmt.execute(H2.getCreateUDAFSQL("AVG_MONTH"))
    stmt.close()

    //Ingestion
    H2.initialize(this, h2storage)
    startIngestion()

    //Interface
    Interface.start(configuration, q => this.executeQuery(connection, q))

    //Shutdown
    connection.close()
    waitUntilIngestionIsDone()
  }

  def getInMemorySegmentGroups(): Iterator[SegmentGroup] = {
    this.cacheLock.readLock().lock()
    val cachedTemporarySegments = this.temporarySegments.values.flatten.toArray
    val cachedFinalizedSegments = this.finalizedSegments.take(this.finalizedSegmentsIndex)
    this.cacheLock.readLock().unlock()
    cachedTemporarySegments.iterator ++ cachedFinalizedSegments.iterator
  }

  /** Private Methods **/
  //Ingestion
  private def startIngestion(): Unit = {
    //Initialize Storage
    val dimensions = configuration.getDimensions
    h2storage.open(dimensions)
    val timeSeries = Partitioner.initializeTimeSeries(configuration, h2storage.getMaxTid)
    val timeSeriesGroups = Partitioner.groupTimeSeries(configuration, timeSeries, h2storage.getMaxGid)
    val derivedTimeSeries = configuration.get("modelardb.source.derived")(0)
      .asInstanceOf[util.HashMap[Integer, Array[Pair[String, ValueFunction]]]]
    h2storage.initialize(timeSeriesGroups, derivedTimeSeries, dimensions, configuration.getModels)
    if (timeSeriesGroups.isEmpty || ! configuration.contains("modelardb.ingestors")) {
      //There are no time series to ingest or no ingestors to ingest them with
      this.numberOfRunningIngestors = new CountDownLatch(0)
      return
    }

    val midCache = h2storage.midCache
    val threads = configuration.getInteger("modelardb.ingestors")
    this.numberOfRunningIngestors = new CountDownLatch(threads)
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
        if (finalizedSegmentsIndex == configuration.getInteger("modelardb.batch")) {
          h2storage.storeSegmentGroups(finalizedSegments, finalizedSegmentsIndex)
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
    h2storage.storeSegmentGroups(finalizedSegments, finalizedSegmentsIndex)
    finalizedSegmentsIndex = 0
    cacheLock.writeLock().unlock()
    workingSet.logger.printWorkingSetResult()
    this.numberOfRunningIngestors.countDown()
  }

  private def updateTemporarySegment(cache: Array[SegmentGroup], inputSG: SegmentGroup, isTemporary: Boolean): Array[SegmentGroup]= {
    //The gaps are extracted from the new finalized or temporary segment
    val inputGaps = Static.bytesToInts(inputSG.offsets)

    //Extracts the metadata for the group of time series being updated
    val groupMetadataCache = h2storage.groupMetadataCache
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

  private def waitUntilIngestionIsDone(): Unit = {
    if (this.numberOfRunningIngestors.getCount != 0) {
      Static.info("ModelarDB: waiting for all ingestors to finnish")
    }
    this.numberOfRunningIngestors.await()
  }

  //Query Processing
  private def executeQuery(connection: Connection, query: String): Array[String] = {
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
}

object H2 {
  /** Instance Variables * */
  var h2: H2 = _ //Provides access to the h2 and h2storage instances from the views
  var h2storage: H2Storage =  _
  private val compareTypeField = classOf[Comparison].getDeclaredField("compareType")
  this.compareTypeField.setAccessible(true)
  private val compareTypeMethod = classOf[Comparison].getDeclaredMethod("getCompareOperator", classOf[Int])
  this.compareTypeMethod.setAccessible(true)
  private val andOrTypeField = classOf[ConditionAndOr].getDeclaredField("andOrType")
  this.andOrTypeField.setAccessible(true)

  /** Public Methods **/
  def initialize(h2: H2, h2Storage: H2Storage): Unit = {
    this.h2 = h2
    this.h2storage = h2Storage
  }

  //Data Point View
  //Documentation: https://www.h2database.com/html/features.html#pluggable_tables
  def getCreateDataPointViewSQL(dimensions: Dimensions): String = {
    s"""CREATE TABLE DataPoint(tid INT, timestamp TIMESTAMP, value REAL${H2.getDimensionColumns(dimensions)})
       |ENGINE "dk.aau.modelardb.engines.h2.ViewDataPoint";
       |""".stripMargin
  }

  //Segment View
  //Documentation: https://www.h2database.com/html/features.html#pluggable_tables
  def getCreateSegmentViewSQL(dimensions: Dimensions): String = {
    s"""CREATE TABLE Segment
       |(tid INT, start_time TIMESTAMP, end_time TIMESTAMP, resolution INT, mid INT, parameters BINARY, gaps BINARY${H2.getDimensionColumns(dimensions)})
       |ENGINE "dk.aau.modelardb.engines.h2.ViewSegment";
       |""".stripMargin
  }

  //Segment View UDAFs
  def getCreateUDAFSQL(sqlName: String): String = {
    val splitSQLName = sqlName.split("_")
    val className = splitSQLName.map(_.toLowerCase.capitalize).mkString("")
    s"""CREATE AGGREGATE $sqlName FOR "dk.aau.modelardb.engines.h2.$className";"""
  }

  def expressionToSQLPredicates(expression: Expression, tsgc: Array[Int], idc: HashMap[String, HashMap[Object, Array[Integer]]],
                                supportsOr: Boolean): String = { //HACK: supportsOR ensures Cassandra does not receive an OR operator
    expression match {
      //NO PREDICATES
      case null => ""
      //COLUMN OPERATOR VALUE
      case c: Comparison =>
        //HACK: Extracts the operator from the sub-tree using reflection as compareType seems to be completely inaccessible
        val operator = this.compareTypeMethod.invoke(c, this.compareTypeField.get(c)).asInstanceOf[String]
        val ec = c.getSubexpression(0).asInstanceOf[ExpressionColumn]
        val ve = c.getSubexpression(1).asInstanceOf[ValueExpression]
        (ec.getColumnName, operator) match {
          //TID
          case ("TID", "=") => val tid = ve.getValue(null).asInstanceOf[ValueInt].getInt
            " GID = " + PredicatePushDown.tidPointToGidPoint(tid, tsgc)
          //TIMESTAMP
          case ("TIMESTAMP", ">") => " END_TIME > " + ve.getValue(null).asInstanceOf[ValueTimestamp].getTimestamp(TimeZone.getDefault).getTime
          case ("TIMESTAMP", ">=") => " END_TIME >= " + ve.getValue(null).asInstanceOf[ValueTimestamp].getTimestamp(TimeZone.getDefault).getTime
          case ("TIMESTAMP", "<") => " STAT_TIME < " + ve.getValue(null).asInstanceOf[ValueTimestamp].getTimestamp(TimeZone.getDefault).getTime
          case ("TIMESTAMP", "<=") => " START_TIME <= " + ve.getValue(null).asInstanceOf[ValueTimestamp].getTimestamp(TimeZone.getDefault).getTime
          case ("TIMESTAMP", "=") =>
            " (START_TIME <= " + ve.getValue(null).asInstanceOf[ValueTimestamp].getTimestamp(TimeZone.getDefault).getTime +
              " AND END_TIME >= " + ve.getValue(null).asInstanceOf[ValueTimestamp].getTimestamp(TimeZone.getDefault).getTime + ")"
          //DIMENSIONS
          case (columnName, "=") if idc.containsKey(columnName) =>
            PredicatePushDown.dimensionEqualToGidIn(columnName, ve.getValue(null).getObject(), idc).mkString("GID IN (", ",", ")")
          case p => Static.warn("ModelarDB: unsupported predicate " + p, 120); ""
        }
      //IN
      case cin: ConditionInConstantSet =>
        cin.getSubexpression(0).getColumnName match {
          case "TID" =>
            val tids = Array.fill[Any](cin.getSubexpressionCount - 1)(0) //The first value is the column name
            for (i <- Range(1, cin.getSubexpressionCount)) {
              tids(i - 1) = cin.getSubexpression(i).getValue(null).asInstanceOf[ValueInt].getInt
            }
            PredicatePushDown.tidInToGidIn(tids, tsgc).mkString("GID IN (", ",", ")")
          case p => Static.warn("ModelarDB: unsupported predicate " + p, 120); ""
        }
      //AND
      case cao: ConditionAndOr if this.andOrTypeField.getInt(cao) == ConditionAndOr.AND =>
        val left = expressionToSQLPredicates(cao.getSubexpression(0), tsgc, idc, supportsOr)
        val right = expressionToSQLPredicates(cao.getSubexpression(1), tsgc, idc, supportsOr)
        if (left == "" || right == "") "" else "(" + left + " AND " + right + ")"
      //OR
      case cao: ConditionAndOr if this.andOrTypeField.getInt(cao) == ConditionAndOr.OR && supportsOr =>
        val left = expressionToSQLPredicates(cao.getSubexpression(0), tsgc, idc, supportsOr)
        val right = expressionToSQLPredicates(cao.getSubexpression(1), tsgc, idc, supportsOr)
        if (left == "" || right == "") "" else "(" + left + " OR " + right + ")"
      case p => Static.warn("ModelarDB: unsupported predicate " + p, 120); ""
    }
  }

  /** Private Methods **/
  private def getDimensionColumns(dimensions: Dimensions): String = {
    if (dimensions.getColumns.isEmpty) {
      ""
    } else {
      dimensions.getColumns.zip(dimensions.getTypes).map {
        case (name, Types.INT) => name + " INT"
        case (name, Types.LONG) => name + " BIGINT"
        case (name, Types.FLOAT) => name + " REAL"
        case (name, Types.DOUBLE) => name + " DOUBLE"
        case (name, Types.TEXT) => name + " VARCHAR"
      }.mkString(", ", ", ", "")
    }
  }
}
