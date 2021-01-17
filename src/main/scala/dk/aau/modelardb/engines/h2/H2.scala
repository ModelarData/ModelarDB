package dk.aau.modelardb.engines.h2

import java.sql.{Connection, DriverManager}

import dk.aau.modelardb.Interface
import dk.aau.modelardb.core.{Dimensions, Storage}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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
      q => this.sql(connection, q)
    )

    //Shutdown
    connection.close()
  }

  /** Private Methods */
  private def sql(connection: Connection, query: String): Array[String] = {
    val stmt = connection.createStatement()
    stmt.execute(query)
    val rs = stmt.getResultSet
    val md = rs.getMetaData

    val result = ArrayBuffer[String]()
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

object H2 {

  /** Public Methods **/
  def initialize(storage: Storage) {
    H2.storage = storage
  }

  def getStorage: Storage = H2.storage

  /** Instance Variables **/
  private var storage: Storage = _
}