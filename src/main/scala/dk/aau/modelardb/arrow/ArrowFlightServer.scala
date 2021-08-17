package dk.aau.modelardb.arrow

import akka.stream.scaladsl.SourceQueueWithComplete
import dk.aau.modelardb.arrow.ArrowUtil.jdbcToArrow
import dk.aau.modelardb.config.{ArrowConfig, ArrowServerConfig}
import dk.aau.modelardb.core.{Dimensions, SegmentGroup, Storage}
import dk.aau.modelardb.engines.QueryEngine
import dk.aau.modelardb.storage.{JDBCStorage, StorageFactory}
import org.apache.arrow.flight.{FlightServer, Location}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot

import java.nio.charset.StandardCharsets
import java.sql.DriverManager
import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.util.{Failure, Success, Try}

class ArrowFlightServer private(host: String, port: Int, queryEngine: QueryEngine, storage: Storage, mode: String) {

  val allocator = new RootAllocator(Long.MaxValue)
  val location = new Location(s"grpc+tcp://$host:$port")

  val producer = new ArrowFlightProducer(queryEngine, storage, mode)

  val server = FlightServer
    .builder(allocator, location, producer)
    .build()

  def start(): FlightServer = {
    Try {
      server.start()
    }.fold[FlightServer](
      {
        case _: IllegalStateException => server
        case throwable: Throwable => throw throwable
      },
      { _ => server }
    )
  }

  def stop(): Unit = {
    server.shutdown()
    server.awaitTermination(10, TimeUnit.SECONDS)
  }
}

object ArrowFlightServer {

  private var instance: ArrowFlightServer = null

  def apply(config: ArrowConfig, sqlEngine: QueryEngine, storage: Storage, mode: String = "edge"): ArrowFlightServer = {
//    if (instance == null) {
//      instance = new ArrowFlightServer(config.server.host, config.server.port, sqlEngine, storage)
//    }
//    instance
    new ArrowFlightServer(config.server.host, config.server.port, sqlEngine, storage, mode)
  }


  /** For use when running code locally. Helps with testing */
  def main(args: Array[String]): Unit = {
    val serverConfig = ArrowServerConfig("localhost", 6006)
    val config = ArrowConfig("/test/path", serverConfig, /*client=*/ null)
    val storage = StorageFactory.getStorage("jdbc:h2:file:./h2db;AUTO_SERVER=TRUE").asInstanceOf[JDBCStorage]
    val dimensions = new Dimensions(Array.empty[String])
    storage.open(dimensions)

    /* Setup Test Data */
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

    val testEngine = new QueryEngine {

      override def start(): Unit = null

      override def start(queue: SourceQueueWithComplete[SegmentGroup]): Unit = ???

      override def stop(): Unit = conn.close()

      override def execute(query: String): VectorSchemaRoot = jdbcToArrow(rs)

    }

    ArrowFlightServer(config, testEngine, storage, "edge")

  }
}
