package dk.aau.modelardb.engines.h2

import java.sql.DriverManager
import dk.aau.modelardb.Interface
import dk.aau.modelardb.core.{Dimensions, Storage}
import dk.aau.modelardb.engines.RDBMSEngineUtilities

class H2(interface: String, engine: String, storage: Storage, dimensions: Dimensions, models: Array[String]) {
  /** Public Methods **/
  def start(): Unit = {
    //Initialize Storage and Engine
    val connection = DriverManager.getConnection("jdbc:h2:mem:")
    storage.open(dimensions)
    storage.initialize(Array(), dimensions, models)
    H2.initialize(storage)

    val stmt = connection.createStatement()
    //Initialize Data Point View
    stmt.execute("CREATE TABLE DataPoint(sid INT, ts TIMESTAMP, val REAL) ENGINE \"dk.aau.modelardb.engines.h2.ViewDataPoint\";")

    //Initialize Segment View
    stmt.execute("CREATE AGGREGATE COUNT_S FOR \"dk.aau.modelardb.engines.h2.CountS\";")
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

object H2 {

  /** Public Methods **/
  def initialize(storage: Storage) {
    H2.storage = storage
  }

  def getStorage: Storage = H2.storage

  /** Instance Variables **/
  private var storage: Storage = _
}