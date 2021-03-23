package dk.aau.modelardb.engines.derby

import java.sql.DriverManager
import dk.aau.modelardb.Interface
import dk.aau.modelardb.core.{Configuration, Storage}
import dk.aau.modelardb.engines.RDBMSEngineUtilities

class Derby(configuration: Configuration, storage: Storage) {
  /** Public Methods **/
  def start(): Unit = {
    //Initialize
    //https://db.apache.org/derby/docs/10.15/security/rsecpolicysample.html
    //https://db.apache.org/derby/docs/10.15/devguide/cdevdvlpinmemdb.html
    val connection = DriverManager.getConnection("jdbc:derby:memory:;create=true")
    val stmt = connection.createStatement()

    //TODO: extend the schema of both views with the columns of the user-defined dimensions at run-time
    //https://db.apache.org/derby/docs/10.15/ref/rrefcreatefunctionstatement.html
    //https://db.apache.org/derby/docs/10.15/ref/rrefsqljexternalname.html
    //https://db.apache.org/derby/docs/10.15/ref/rrefsqlj15446.html
    stmt.execute("""CREATE FUNCTION Segment()
                   |RETURNS TABLE (sid INT, start_time TIMESTAMP, end_time TIMESTAMP, resolution INT, mid INT, parameters BLOB, gaps BLOB)
                   |LANGUAGE JAVA
                   |PARAMETER STYLE DERBY_JDBC_RESULT_SET
                   |READS SQL DATA
                   |EXTERNAL NAME 'dk.aau.modelardb.engines.derby.ViewSegment.apply'
                   |""".stripMargin)
    stmt.execute("CREATE VIEW Segment as SELECT s.* FROM TABLE(Segment()) s")

    stmt.execute("""CREATE FUNCTION DataPoint()
                   |RETURNS TABLE (sid INT, timestamp TIMESTAMP, value REAL)
                   |LANGUAGE JAVA PARAMETER STYLE DERBY_JDBC_RESULT_SET
                   |READS SQL DATA
                   |EXTERNAL NAME 'dk.aau.modelardb.engines.derby.ViewDataPoint.apply'""".stripMargin)
    stmt.execute("CREATE VIEW DataPoint as SELECT d.* FROM TABLE(DataPoint()) d")

    //https://db.apache.org/derby/docs/10.15/ref/rrefsqljcreatetype.html
    //https://db.apache.org/derby/docs/10.15/ref/rrefsqljexternalname.html
    stmt.execute("""CREATE TYPE segment
                   |EXTERNAL NAME 'dk.aau.modelardb.engines.derby.Segment'
                   |LANGUAGE JAVA
                   |""".stripMargin)
    //https://db.apache.org/derby/docs/10.15/ref/rrefcreatefunctionstatement.html
    //https://db.apache.org/derby/docs/10.15/ref/rrefsqljexternalname.html
    stmt.execute("""CREATE FUNCTION to_segment(sid INT, start_time TIMESTAMP, end_time TIMESTAMP, resolution INT, mid INT, parameters BLOB, gaps BLOB)
                   |RETURNS segment
                   |PARAMETER STYLE JAVA NO SQL
                   |LANGUAGE JAVA
                   |EXTERNAL NAME 'dk.aau.modelardb.engines.derby.Segment.toSegment'""".stripMargin)
    //https://db.apache.org/derby/docs/10.15/ref/rrefsqljcreateaggregate.html
    //https://db.apache.org/derby/docs/10.15/ref/rrefsqljexternalname.html
    stmt.execute("CREATE DERBY AGGREGATE count_big FOR int RETURNS bigint EXTERNAL NAME 'dk.aau.modelardb.engines.derby.CountBig'")
    stmt.execute("CREATE DERBY AGGREGATE count_s FOR segment RETURNS bigint EXTERNAL NAME 'dk.aau.modelardb.engines.derby.CountS'")
    stmt.close()

    //Ingestion
    RDBMSEngineUtilities.initialize(configuration, storage)
    val utilities = RDBMSEngineUtilities.getUtilities
    utilities.startIngestion()

    //Interface
    Interface.start(configuration, q => utilities.executeQuery(connection, q))

    //Shutdown
    connection.close()
    RDBMSEngineUtilities.waitUntilIngestionIsDone()
  }
}