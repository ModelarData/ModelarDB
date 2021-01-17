package dk.aau.modelardb.engines.derby

import dk.aau.modelardb.Interface
import dk.aau.modelardb.core.{Dimensions, Storage}
import java.sql.{Connection, DriverManager, ResultSet}

import scala.collection.mutable

class Derby(interface: String, engine: String, storage: Storage, dimensions: Dimensions, models: Array[String]) {
  /** Public Methods **/
  def start(): Unit = {
    //Initialize Storage and Engine
    System.setSecurityManager(null) //HACK: security manager is disabled during development
    val connection = DriverManager.getConnection("jdbc:derby:memory:tempdb;create=true")
    storage.open(dimensions)
    storage.initialize(Array(), dimensions, models)
    Derby.initialize(storage)

    val stmt = connection.createStatement()
    //Initialize Data Point View
    stmt.execute("CREATE FUNCTION DataPoint() RETURNS TABLE (sid INT, ts TIMESTAMP, val FLOAT) LANGUAGE JAVA PARAMETER STYLE DERBY_JDBC_RESULT_SET READS SQL DATA EXTERNAL NAME 'dk.aau.modelardb.engines.derby.Derby.dataPointViewRestricted'")
    stmt.execute("CREATE VIEW DataPoint as SELECT s.* FROM TABLE (DataPoint() ) s")

    //Initialize Segment View
    stmt.execute("CREATE TYPE segment EXTERNAL NAME 'dk.aau.modelardb.engines.derby.Segment' LANGUAGE JAVA")
    stmt.execute("CREATE FUNCTION TO_SEGMENT(gid INTEGER, start_time BIGINT, end_time BIGINT, mid INTEGER, params BLOB, gaps BLOB) RETURNS segment PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA EXTERNAL NAME 'dk.aau.modelardb.engines.derby.Segment.toSegment'")
    stmt.execute("CREATE DERBY AGGREGATE count_s FOR segment EXTERNAL NAME 'dk.aau.modelardb.engines.derby.COUNT_S'")
    stmt.close()

    //Interface
    Interface.start(
      interface,
      q => this.sql(connection, q)
    )

    //Shutdown
    connection.close()
  }

  /** Private Methods */
  def sql(connection: Connection, query: String): Array[String] = {
    //Execute the query
    val stmt = connection.createStatement()
    stmt.execute(query)
    val rs = stmt.getResultSet
    val md = rs.getMetaData

    val result = mutable.ArrayBuffer[String]()
    while (rs.next()) {
      val row = mutable.HashMap[String, String]()
      for (i <- 1 to md.getColumnCount) {
        row(md.getColumnName(i)) = rs.getString(i)
      }
      result.append(row.toString())
    }
    rs.close()
    stmt.close()
    result.toArray
  }
}

object Derby {

  /** Constructors **/
  def initialize(storage: Storage): Unit = {
    Derby.storage = storage
  }

  /** Public Methods **/
  def dataPointView(): ResultSet = {
    new DerbyResultSet()
  }

  def dataPointViewRestricted(): DerbyResultSet = {
    new DerbyResultSet()
  }

  def getStorage: Storage = Derby.storage

  /** Instance Variables **/
  private var storage: Storage = _
}