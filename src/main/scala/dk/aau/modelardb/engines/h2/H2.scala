package dk.aau.modelardb.engines.h2

import java.sql.DriverManager
import dk.aau.modelardb.Interface
import dk.aau.modelardb.core.{Configuration, Storage}
import dk.aau.modelardb.engines.RDBMSEngineUtilities
import dk.aau.modelardb.engines.h2.H2.{CreateDataPointViewSQL, CreateSegmentViewSQL}

class H2(configuration: Configuration, storage: Storage) {
  /** Public Methods **/
  def start(): Unit = {
    //Engine
    //http://www.h2database.com/html/features.html#in_memory_databases
    val connection = DriverManager.getConnection("jdbc:h2:mem:")
    val stmt = connection.createStatement()
    //https://www.h2database.com/html/commands.html#create_table
    stmt.execute(CreateDataPointViewSQL)
    stmt.execute(CreateSegmentViewSQL)
    //https://www.h2database.com/html/commands.html#create_aggregate
    stmt.execute("CREATE AGGREGATE COUNT_S FOR \"dk.aau.modelardb.engines.h2.CountS\";")
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

object H2 {
  val CreateDataPointViewSQL  =
    """CREATE TABLE DataPoint(sid INT, ts TIMESTAMP, val REAL)
      |ENGINE "dk.aau.modelardb.engines.h2.ViewDataPoint";
      |""".stripMargin

  val CreateSegmentViewSQL    =
    """CREATE TABLE Segment(sid INT, start_time TIMESTAMP, end_time TIMESTAMP, resolution INT)
      |ENGINE "dk.aau.modelardb.engines.h2.SegmentView";
      |""".stripMargin
}