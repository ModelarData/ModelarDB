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
package dk.aau.modelardb.storage

import java.sql.{Array => _, _}
import java.util
import scala.collection.JavaConverters._
import dk.aau.modelardb.core._
import dk.aau.modelardb.core.utility.{Pair, Static, ValueFunction}
import dk.aau.modelardb.engines.derby.{Derby, DerbyStorage}
import org.apache.derby.vti.Restriction
import dk.aau.modelardb.engines.h2.{H2, H2Storage}
import org.h2.table.TableFilter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{Row, SparkSession}
import dk.aau.modelardb.engines.spark.SparkStorage

import scala.collection.mutable.ListBuffer

class JDBCStorage(connectionStringAndTypes: String) extends Storage with DerbyStorage with H2Storage with SparkStorage {

  /** Public Methods **/
  //Storage
  override def open(dimensions: Dimensions): Unit = {
    //Initializes the RDBMS connection
    this.connection = DriverManager.getConnection(connectionString)
    this.connection.setAutoCommit(false)

    //Checks if the tables exist and create them if necessary
    val metadata = this.connection.getMetaData
    val tableType = Array("TABLE")
    val tables = metadata.getTables(null, null, "SEGMENT", tableType)

    if ( ! tables.next()) {
      val stmt = this.connection.createStatement()
      stmt.executeUpdate(s"CREATE TABLE model(mid INTEGER, name ${this.textType})")
      stmt.executeUpdate(s"CREATE TABLE segment(gid INTEGER, start_time BIGINT, end_time BIGINT, mid INTEGER, params ${this.blobType}, gaps ${this.blobType})")
      stmt.executeUpdate(s"CREATE TABLE source(sid INTEGER, scaling FLOAT, resolution INTEGER, gid INTEGER${dimensions.getSchema(this.textType)})")
    }

    //Prepares the necessary statements
    this.insertStmt = this.connection.prepareStatement("INSERT INTO segment VALUES(?, ?, ?, ?, ?, ?)")
    this.getMaxSidStmt = this.connection.prepareStatement("SELECT MAX(sid) FROM source")
    this.getMaxGidStmt = this.connection.prepareStatement("SELECT MAX(gid) FROM source")
  }

  override def initialize(timeSeriesGroups: Array[TimeSeriesGroup],
                          derivedTimeSeries: util.HashMap[Integer, Array[Pair[String, ValueFunction]]],
                          dimensions: Dimensions, modelNames: Array[String]): Unit = {
    //Inserts the metadata for the sources defined in the configuration file (Sid, Resolution, Gid, Dimensions)
    val sourceDimensions = dimensions.getColumns.length
    val columns = "?, " * (sourceDimensions + 3) + "?"
    val insertSourceStmt = connection.prepareStatement("INSERT INTO source VALUES(" + columns + ")")
    for (tsg <- timeSeriesGroups) {
      for (ts <- tsg.getTimeSeries) {
        insertSourceStmt.clearParameters()
        insertSourceStmt.setInt(1, ts.sid)
        insertSourceStmt.setFloat(2, ts.scalingFactor)
        insertSourceStmt.setInt(3, ts.resolution)
        insertSourceStmt.setInt(4, tsg.gid)

        var column = 5
        for (dim <- dimensions.get(ts.source)) {
          //TODO: Support none string members
          insertSourceStmt.setString(column, dim.toString)
          column += 1
        }
        insertSourceStmt.executeUpdate()
      }
    }

    //Extracts the scaling factor, resolution, gid, and dimensions for the sources in storage
    var stmt = this.connection.createStatement()
    var results = stmt.executeQuery("SELECT * FROM source")
    val sourcesInStorage = new util.HashMap[Integer, Array[Object]]()
    while (results.next) {
      //The metadata is stored as (Sid => Scaling, Resolution, Gid, Dimensions)
      val sid = results.getInt(1)
      val metadata = new util.ArrayList[Object]()
      metadata.add(results.getFloat(2).asInstanceOf[Object]) //Scaling
      metadata.add(results.getInt(3).asInstanceOf[Object]) //Resolution
      metadata.add(results.getInt(4).asInstanceOf[Object]) //Gid

      //Dimensions
      var column = 5
      while(column <= sourceDimensions + 4) {
        //TODO: Support none string members
        metadata.add(results.getString(column))
        column += 1
      }
      sourcesInStorage.put(sid, metadata.toArray)
    }


    //Extracts the name of all models in storage
    stmt = this.connection.createStatement()
    results = stmt.executeQuery("SELECT * FROM model")
    val modelsInStorage = new util.HashMap[String, Integer]()
    while (results.next) {
      modelsInStorage.put(results.getString(2), results.getInt(1))
    }

    //Initializes the caches managed by Storage
    val modelsToInsert = super.initializeCaches(modelNames, dimensions, modelsInStorage, sourcesInStorage, derivedTimeSeries)

    //Inserts the name of each model in the configuration file but not in the model table
    val insertModelStmt = connection.prepareStatement("INSERT INTO model VALUES(?, ?)")
    for ((k, v) <- modelsToInsert.asScala) {
      insertModelStmt.clearParameters()
      insertModelStmt.setInt(1, v)
      insertModelStmt.setString(2, k)
      insertModelStmt.executeUpdate()
    }
  }

  override def getMaxSID(): Int = {
    getFirstInteger(this.getMaxSidStmt)
  }

  override def getMaxGID(): Int = {
    getFirstInteger(this.getMaxGidStmt)
  }

  override def close(): Unit = {
    //Connection cannot be closed while a transaction is running
    this.connection.commit()
    this.connection.close()
  }

  //DerbyStorage
  override def storeSegmentGroups(segments: Array[SegmentGroup], size: Int): Unit = {
    try {
      for (segmentGroup <- segments.take(size)) {
        this.insertStmt.setInt(1, segmentGroup.gid)
        this.insertStmt.setLong(2, segmentGroup.startTime)
        this.insertStmt.setLong(3, segmentGroup.endTime)
        this.insertStmt.setInt(4, segmentGroup.mid)
        this.insertStmt.setBytes(5, segmentGroup.parameters)
        this.insertStmt.setBytes(6, segmentGroup.offsets)
        this.insertStmt.addBatch()
      }
      this.insertStmt.executeBatch()
      this.connection.commit()
    } catch {
      case se: java.sql.SQLException =>
        close()
        throw new java.lang.RuntimeException(se)
    }
  }

  override def getSegmentGroups(filter: Restriction): Iterator[SegmentGroup] = {
    val sql = Derby.toSQL(filter, this)
    val rs = connection
      .createStatement()
      .executeQuery(sql)

    val groups = ListBuffer.empty[SegmentGroup]

    while (rs.next()) {
      groups += resultSetToSegmentGroup(rs)
    }

    groups.iterator
  }

  //H2Storage
  override def getSegmentGroups(filter: TableFilter): Iterator[SegmentGroup] = {
    getSegmentGroups(H2.tableFilterToSQLPredicates(filter, this.sourceGroupCache))
  }

  //SparkStorage
  override def open(ssb: SparkSession.Builder, dimensions: Dimensions): SparkSession = {
    open(dimensions)
    ssb.getOrCreate()
  }

  override def storeSegmentGroups(sparkSession: SparkSession, rdd: RDD[Row]): Unit = {
    val groups = rdd.map(row => new SegmentGroup(row.getInt(0), row.getTimestamp(1).getTime,
      row.getTimestamp(2).getTime, row.getInt(3), row.getAs[Array[Byte]](4), row.getAs[Array[Byte]](5))).collect()
    storeSegmentGroups(groups, groups.length)
  }

  override def getSegmentGroups(sparkSession: SparkSession, filters: Array[Filter]): RDD[Row] = {
    Static.warn("ModelarDB: projection and predicate push-down is not yet implemented")
    val rows = getSegmentGroups("").map(sg => {
      Row(sg.gid, new Timestamp(sg.startTime), new Timestamp(sg.endTime), sg.mid, sg.parameters, sg.offsets)
    })
    sparkSession.sparkContext.parallelize(rows.toSeq)
  }

  /** Private Methods **/
   private def splitConnectionStringAndTypes(connectionStringWithArguments: String): (String, String, String) = {
     val split = connectionStringWithArguments.split(" ")
     if (split.length == 3) {
       (split(0), split(1), split(2))
     } else {
       val rdbms = connectionStringWithArguments.split(":")(1)
       val defaults = Map(
         "sqlite" -> Tuple3(connectionStringWithArguments, "TEXT", "BYTEA"),
         "postgresql" -> Tuple3(connectionStringWithArguments, "TEXT", "BYTEA"),
         "derby" -> Tuple3(connectionStringWithArguments, "LONG VARCHAR", "LONG VARCHAR FOR BIT DATA"),
         "h2" -> Tuple3(connectionStringWithArguments, "VARCHAR", "BINARY"),
         "hsqldb" -> Tuple3(connectionStringWithArguments, "LONGVARCHAR", "LONGVARBINARY"))
       if ( ! defaults.contains(rdbms)) {
         throw new IllegalArgumentException("ModelarDB: the string and binary type must also be specified for " + rdbms)
       }
       defaults(rdbms)
     }
   }

  private def getSegmentGroups(predicates: String): Iterator[SegmentGroup] = {
    val stmt = this.connection.createStatement()
    val results = if (predicates.isEmpty) {
      stmt.executeQuery("SELECT * FROM segment")
    } else {
      Static.info(s"ModelarDB: constructed predicates ($predicates)")
      stmt.executeQuery("SELECT * FROM segment WHERE " + predicates)
    }
    new Iterator[SegmentGroup] {
      override def hasNext: Boolean = {
        if (results.next()) {
          true
        } else {
          results.close()
          stmt.close()
          false
        }
      }
      override def next(): SegmentGroup = resultSetToSegmentGroup(results)
    }
  }

  private def resultSetToSegmentGroup(resultSet: ResultSet): SegmentGroup = {
    val gid = resultSet.getInt(1)
    val startTime = resultSet.getLong(2)
    val endTime = resultSet.getLong(3)
    val mid = resultSet.getInt(4)
    val params = resultSet.getBytes(5)
    val gaps = resultSet.getBytes(6)
    new SegmentGroup(gid, startTime, endTime, mid, params, gaps)
  }

  def getFirstInteger(query: PreparedStatement): Int = {
    try {
      val results = query.executeQuery()
      results.next
      results.getInt(1)
    } catch {
      case se: java.sql.SQLException =>
        close()
        throw new java.lang.RuntimeException(se)
    }
  }

  /** Instance Variables **/
  private var connection: Connection = _
  private var insertStmt: PreparedStatement = _
  private var getMaxSidStmt: PreparedStatement = _
  private var getMaxGidStmt: PreparedStatement = _
  private val (connectionString, textType, blobType) = splitConnectionStringAndTypes(connectionStringAndTypes)
}