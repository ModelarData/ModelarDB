package dk.aau.modelardb.engines.derby

import java.sql.{DriverManager, ResultSet}

import dk.aau.modelardb.Interface
import dk.aau.modelardb.core.{Dimensions, Storage}
import dk.aau.modelardb.engines.RDBMSEngineUtilities

class Derby(interface: String, engine: String, storage: Storage, dimensions: Dimensions, models: Array[String], batchSize: Int) {
  /** Public Methods **/
  def start(): Unit = {
    //Initialize
    System.setSecurityManager(null) //HACK: security manager is disabled during development
    //https://db.apache.org/derby/docs/10.15/devguide/cdevdvlpinmemdb.html
    val connection = DriverManager.getConnection("jdbc:derby:memory:tempdb;create=true")
    val stmt = connection.createStatement()
    //https://db.apache.org/derby/docs/10.15/ref/rrefcreatefunctionstatement.html
    //https://db.apache.org/derby/docs/10.15/ref/rrefsqljexternalname.html
    stmt.execute("CREATE FUNCTION DataPoint() RETURNS TABLE (sid INT, ts TIMESTAMP, val FLOAT) LANGUAGE JAVA PARAMETER STYLE DERBY_JDBC_RESULT_SET READS SQL DATA EXTERNAL NAME 'dk.aau.modelardb.engines.derby.Derby.dataPointView'")
    //https://db.apache.org/derby/docs/10.15/ref/rrefsqlj15446.html
    stmt.execute("CREATE VIEW DataPoint as SELECT s.* FROM TABLE(DataPoint()) s")

    //https://db.apache.org/derby/docs/10.15/ref/rrefsqljcreatetype.html
    //https://db.apache.org/derby/docs/10.15/ref/rrefsqljexternalname.html
    stmt.execute("CREATE TYPE segment EXTERNAL NAME 'dk.aau.modelardb.engines.derby.Segment' LANGUAGE JAVA")
    //https://db.apache.org/derby/docs/10.15/ref/rrefcreatefunctionstatement.html
    //https://db.apache.org/derby/docs/10.15/ref/rrefsqljexternalname.html
    stmt.execute("CREATE FUNCTION TO_SEGMENT(gid INTEGER, start_time BIGINT, end_time BIGINT, mid INTEGER, params BLOB, gaps BLOB) RETURNS segment PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA EXTERNAL NAME 'dk.aau.modelardb.engines.derby.Segment.toSegment'")
    //https://db.apache.org/derby/docs/10.15/ref/rrefsqljcreateaggregate.html
    //https://db.apache.org/derby/docs/10.15/ref/rrefsqljexternalname.html
    stmt.execute("CREATE DERBY AGGREGATE count_s FOR segment EXTERNAL NAME 'dk.aau.modelardb.engines.derby.CountS'")
    stmt.close()

    //Ingestion
    RDBMSEngineUtilities.initialize(storage, models, batchSize)
    val utilities = RDBMSEngineUtilities.getUtilities
    utilities.startIngestion(dimensions)

    //Interface
    Interface.start(
      interface,
      q => utilities.executeQuery(connection, q)
    )

    //Shutdown
    connection.close()
  }
}

object Derby {
  //https://db.apache.org/derby/docs/10.15/devguide/cdevspecialtfbasic.html
  //https://db.apache.org/derby/docs/10.15/devguide/cdevspecialtfexample.html
  def dataPointView: ResultSet = {
    new ViewDataPoint()
  }
}