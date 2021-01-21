package dk.aau.modelardb.engines.derby

import java.sql.{DriverManager, ResultSet}

import dk.aau.modelardb.Interface
import dk.aau.modelardb.core.{Dimensions, Storage}
import dk.aau.modelardb.engines.RDBMSEngineUtilities

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
    stmt.execute("CREATE FUNCTION DataPoint() RETURNS TABLE (sid INT, ts TIMESTAMP, val FLOAT) LANGUAGE JAVA PARAMETER STYLE DERBY_JDBC_RESULT_SET READS SQL DATA EXTERNAL NAME 'dk.aau.modelardb.engines.derby.Derby.dataPointView'")
    stmt.execute("CREATE VIEW DataPoint as SELECT s.* FROM TABLE (DataPoint() ) s")

    //Initialize Segment View
    stmt.execute("CREATE TYPE segment EXTERNAL NAME 'dk.aau.modelardb.engines.derby.Segment' LANGUAGE JAVA")
    stmt.execute("CREATE FUNCTION TO_SEGMENT(gid INTEGER, start_time BIGINT, end_time BIGINT, mid INTEGER, params BLOB, gaps BLOB) RETURNS segment PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA EXTERNAL NAME 'dk.aau.modelardb.engines.derby.Segment.toSegment'")
    stmt.execute("CREATE DERBY AGGREGATE count_s FOR segment EXTERNAL NAME 'dk.aau.modelardb.engines.derby.CountS'")
    stmt.close()

    //Interface
    Interface.start(
      interface,
      q => RDBMSEngineUtilities.sql(connection, q)
    )

    //Shutdown
    connection.close()
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

  def getStorage: Storage = Derby.storage

  /** Instance Variables **/
  private var storage: Storage = _
}