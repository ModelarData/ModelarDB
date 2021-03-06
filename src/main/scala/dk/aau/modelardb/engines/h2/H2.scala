package dk.aau.modelardb.engines.h2

import java.sql.DriverManager

import dk.aau.modelardb.Interface
import dk.aau.modelardb.core.{Dimensions, Storage}
import dk.aau.modelardb.engines.RDBMSEngineUtilities

class H2(interface: String, engine: String, storage: Storage, dimensions: Dimensions, models: Array[String], batchSize: Int) {
  /** Public Methods **/
  def start(): Unit = {
    //Engine
    //http://www.h2database.com/html/features.html#in_memory_databases
    val connection = DriverManager.getConnection("jdbc:h2:mem:")
    val stmt = connection.createStatement()
    //https://www.h2database.com/html/commands.html#create_table
    stmt.execute("CREATE TABLE DataPoint(sid INT, ts TIMESTAMP, val REAL) ENGINE \"dk.aau.modelardb.engines.h2.ViewDataPoint\";")
    //https://www.h2database.com/html/commands.html#create_aggregate
    stmt.execute("CREATE AGGREGATE COUNT_S FOR \"dk.aau.modelardb.engines.h2.CountS\";")
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