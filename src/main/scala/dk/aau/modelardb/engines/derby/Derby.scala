package dk.aau.modelardb.engines.derby

import java.sql.DriverManager
import dk.aau.modelardb.Interface
import dk.aau.modelardb.core.{Configuration, Storage}
import dk.aau.modelardb.engines.RDBMSEngineUtilities
import dk.aau.modelardb.engines.derby.Derby.{CreateSegmentFunctionSQL, CreateSegmentTypeSQL, CreateSegmentViewSQL}

class Derby(configuration: Configuration, storage: Storage) {
  /** Public Methods **/
  def start(): Unit = {
    //Initialize
    //https://db.apache.org/derby/docs/10.15/security/rsecpolicysample.html
    //https://db.apache.org/derby/docs/10.15/devguide/cdevdvlpinmemdb.html
    val connection = DriverManager.getConnection("jdbc:derby:memory:tempdb;create=true")
    val stmt = connection.createStatement()
    //https://db.apache.org/derby/docs/10.15/ref/rrefcreatefunctionstatement.html
    //https://db.apache.org/derby/docs/10.15/ref/rrefsqljexternalname.html
    stmt.execute("CREATE FUNCTION DataPoint() RETURNS TABLE (sid INT, ts TIMESTAMP, val FLOAT) LANGUAGE JAVA PARAMETER STYLE DERBY_JDBC_RESULT_SET READS SQL DATA EXTERNAL NAME 'dk.aau.modelardb.engines.derby.ViewDataPoint.dataPointView'")
    stmt.execute(CreateSegmentFunctionSQL)
    //https://db.apache.org/derby/docs/10.15/ref/rrefsqlj15446.html
    stmt.execute("CREATE VIEW DataPoint as SELECT d.* FROM TABLE(DataPoint()) d")
    stmt.execute(CreateSegmentViewSQL)

    //https://db.apache.org/derby/docs/10.15/ref/rrefsqljcreatetype.html
    //https://db.apache.org/derby/docs/10.15/ref/rrefsqljexternalname.html
    stmt.execute(CreateSegmentTypeSQL)
    //https://db.apache.org/derby/docs/10.15/ref/rrefcreatefunctionstatement.html
    //https://db.apache.org/derby/docs/10.15/ref/rrefsqljexternalname.html
    stmt.execute("CREATE FUNCTION TO_SEGMENT(gid INTEGER, start_time BIGINT, end_time BIGINT, mid INTEGER, params BLOB, gaps BLOB) RETURNS segment PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA EXTERNAL NAME 'dk.aau.modelardb.engines.derby.Segment.toSegment'")
    //https://db.apache.org/derby/docs/10.15/ref/rrefsqljcreateaggregate.html
    //https://db.apache.org/derby/docs/10.15/ref/rrefsqljexternalname.html
    stmt.execute("CREATE DERBY AGGREGATE count_s FOR segment EXTERNAL NAME 'dk.aau.modelardb.engines.derby.CountS'")
    stmt.close()

    //Ingestion
    RDBMSEngineUtilities.initialize(configuration, storage)
    val utilities = RDBMSEngineUtilities.getUtilities
    utilities.startIngestion()

    //Interface
    Interface.start(
      configuration.getString("modelardb.interface"),
      q => utilities.executeQuery(connection, q)
    )

    //Shutdown
    connection.close()
  }
}

object Derby {
  val CreateSegmentFunctionSQL =
    """CREATE FUNCTION Segment()
      |RETURNS TABLE (sid INT, start_time TIMESTAMP, end_time TIMESTAMP, resolution INT)
      |LANGUAGE JAVA
      |PARAMETER STYLE DERBY_JDBC_RESULT_SET
      |READS SQL DATA
      |EXTERNAL NAME 'dk.aau.modelardb.engines.derby.SegmentView.apply'
      |""".stripMargin

  val CreateSegmentViewSQL = "CREATE VIEW Segment as SELECT s.* FROM TABLE(Segment()) s"

  val CreateSegmentTypeSQL =
    """CREATE TYPE segment
      |EXTERNAL NAME 'dk.aau.modelardb.engines.derby.Segment'
      |LANGUAGE JAVA
      |""".stripMargin
}