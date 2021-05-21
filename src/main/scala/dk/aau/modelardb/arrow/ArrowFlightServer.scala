package dk.aau.modelardb.arrow

import akka.stream.scaladsl.SourceQueueWithComplete
import dk.aau.modelardb.ConfigUtil
import dk.aau.modelardb.arrow.ArrowUtil.jdbcToArrow
import dk.aau.modelardb.core.{SegmentGroup, Storage}
import dk.aau.modelardb.engines.{QueryEngine, RDBMSEngineUtilities}
import dk.aau.modelardb.storage.{JDBCStorage, StorageFactory}
import org.apache.arrow.flight.{FlightServer, Location}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot

import java.nio.charset.StandardCharsets
import java.sql.DriverManager
import java.time.Instant

class ArrowFlightServer private(queryEngine: QueryEngine, storage: Storage) {

  val allocator = new RootAllocator(Long.MaxValue)
  val location = new Location("grpc+tcp://localhost:6006")

  val producer = new ArrowFlightProducer(queryEngine, storage)

  val server = FlightServer
    .builder(allocator, location, producer)
    .build()

  def start(): FlightServer = {
    server.start()
  }
  def stop(): Unit = {
    server.shutdown()
  }
}

object ArrowFlightServer {

  private var instance: ArrowFlightServer = null

  def apply(sqlEngine: QueryEngine, storage: Storage): ArrowFlightServer = {
    if (instance == null) {
      instance = new ArrowFlightServer(sqlEngine, storage)
    }
    instance
  }


  /** For use when running code locally. Helps with testing */
  def main(args: Array[String]): Unit = {

    /* Setup Test Data */
    val config = ConfigUtil.readConfigurationFile(args(0))
    val storage = StorageFactory.getStorage("jdbc:h2:file:./h2db;AUTO_SERVER=TRUE").asInstanceOf[JDBCStorage]
    val dbUtils = RDBMSEngineUtilities(config, storage)
    val dimensions = config.getDimensions
    storage.open(dimensions)

    val conn = DriverManager.getConnection("jdbc:h2:mem:")

    val updateStmt = conn.createStatement()
    updateStmt.executeUpdate(SegmentSchema.createTableSQL)
    updateStmt.close()

    val insertStmt = conn.prepareStatement("INSERT INTO segment VALUES(?, ?, ?, ?, ?, ?, ?)")
    val rng = scala.util.Random
    (1 to 2050).foreach { id =>
      val start = Instant.now()
      val end = start.plusSeconds(rng.nextInt(100))
      insertStmt.setInt(1, id)
      insertStmt.setTimestamp(2, java.sql.Timestamp.from(start))
      insertStmt.setTimestamp(3, java.sql.Timestamp.from(end))
      insertStmt.setInt(4, 100)
      insertStmt.setInt(5, 123)
      insertStmt.setBytes(6, "params".getBytes(StandardCharsets.UTF_8))
      insertStmt.setBytes(7, "gaps".getBytes(StandardCharsets.UTF_8))
      insertStmt.addBatch()
    }
    insertStmt.executeBatch()
    insertStmt.close()

    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("select * from segment")

    val testData = new QueryEngine {

      override def start(): Unit = null

      override def start(queue: SourceQueueWithComplete[SegmentGroup]): Unit = ???

      override def stop(): Unit = conn.close()

      override def execute(query: String): VectorSchemaRoot = jdbcToArrow(rs)

    }

    ArrowFlightServer(testData, dbUtils.storage)

  }
}
