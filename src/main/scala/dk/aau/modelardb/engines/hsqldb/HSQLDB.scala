package dk.aau.modelardb.engines.hsqldb

import java.sql.DriverManager

import dk.aau.modelardb.Interface
import dk.aau.modelardb.core.{Dimensions, Storage}
import dk.aau.modelardb.engines.RDBMSEngineUtilities

class HSQLDB(interface: String, engine: String, storage: Storage, dimensions: Dimensions, models: Array[String], batchSize: Int) {
  /** Public Methods **/
  def start(): Unit = {
    //Engine
    val connection = DriverManager.getConnection("jdbc:hsqldb:mem:memdb")
    val stmt = connection.createStatement()
    stmt.execute("CREATE FUNCTION DataPoint() RETURNS TABLE(i INTEGER) READS SQL DATA LANGUAGE JAVA EXTERNAL NAME 'CLASSPATH:dk.aau.modelardb.engines.hsqldb.ViewDataPoint.queryView'")
    stmt.execute("CREATE VIEW DataPoint as SELECT * FROM TABLE(DataPoint())")
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


