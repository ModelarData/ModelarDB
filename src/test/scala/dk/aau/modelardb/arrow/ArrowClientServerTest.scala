package dk.aau.modelardb.arrow

import dk.aau.modelardb.TestUtil
import dk.aau.modelardb.config.{ArrowClientConfig, ArrowConfig, ArrowServerConfig}
import dk.aau.modelardb.core.SegmentGroup
import dk.aau.modelardb.engines.QueryEngine
import dk.aau.modelardb.engines.h2.H2Storage
import dk.aau.modelardb.storage.{JDBCStorage, Storage}
import org.apache.arrow.flight.Ticket
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VectorSchemaRoot, VectorUnloader}
import org.scalactic.Equality
import org.scalamock.matchers.ArgCapture.CaptureOne
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.nio.charset.StandardCharsets

class ArrowClientServerTest extends AnyFlatSpec with should.Matchers with MockFactory {
  import TestUtil.segmentGroupEq

  val testData = TestUtil.generateSegmentGroups(10).toArray

  "Arrow Flight Server" should "be able to handle doPut from client" in {
    val randomPort = scala.util.Random.nextInt(60000) + 1024
    val clientConfig = ArrowClientConfig("localhost", randomPort, "test-edge")
    val serverConfig = ArrowServerConfig("localhost", randomPort)
    val flightPath = "/arrow/flight/test/path"
    val testConfig = ArrowConfig(flightPath, serverConfig, clientConfig)

    val queryEngine = mock[QueryEngine]

    val c1 = CaptureOne[Array[SegmentGroup]]()
    val storage = mock[H2StorageNoArgs]
    (storage.storeSegmentGroups(_: Array[SegmentGroup], _: Int))
      .expects(capture(c1), testData.length)

    val arrowServer = ArrowFlightServer(testConfig, queryEngine, storage, "server")
    arrowServer.start()
    val arrowClient = ArrowFlightClient(testConfig)
    arrowClient.doPut(testData)
    arrowServer.stop()
    arrowClient.client.close()

    val actual = c1.value
    actual should have size testData.length
    actual.zip(testData).foreach{ case (sg1, sg2) =>
      sg1 should equal(sg2)
    }
  }

  it should "answer query" in {
    val randomPort = scala.util.Random.nextInt(60000) + 1024
    val clientConfig = ArrowClientConfig("localhost", randomPort, "test-edge")
    val serverConfig = ArrowServerConfig("localhost", randomPort)
    val flightPath = "/arrow/flight/test/path"
    val testConfig = ArrowConfig(flightPath, serverConfig, clientConfig)

    val sql = "select * from segment"

    val storage = mock[H2StorageNoArgs]
    val queryEngine = mock[QueryEngine]
    val testDataRoot = VectorSchemaRoot.create(SegmentSchema.arrowSchema, new RootAllocator(Long.MaxValue))

    testData.zipWithIndex.foreach { case (sg, i) =>
      ArrowUtil.addToRoot(i, sg, testDataRoot)
    }
    testDataRoot.setRowCount(testData.length)

    (queryEngine.execute _)
      .expects(sql)
      .returns(testDataRoot)

    val arrowServer = ArrowFlightServer(testConfig, queryEngine, storage)
    arrowServer.start()
    val arrowClient = ArrowFlightClient(testConfig)
    val grpcClient = arrowClient.client
    val ticket = new Ticket(sql.getBytes(StandardCharsets.UTF_8))
    val stream = grpcClient.getStream(ticket)
    while (!stream.hasRoot) {}
    val schema = stream.getSchema
    val resultRoot = VectorSchemaRoot.create(schema, new RootAllocator(Long.MaxValue))

    var totalRows = 0;
    while (stream.next()) {
      val foreignRoot = stream.getRoot
      val rows = foreignRoot.getRowCount
      val cols = schema.getFields.size()
      (0 until cols).foreach{ i =>
        val vector = foreignRoot.getVector(i)
        resultRoot.addVector(i, vector)
      }
      totalRows += rows
    }
    resultRoot.setRowCount(totalRows)

    totalRows should equal (testData.length)
  }

  /* HACK: needed because Storage class does not have a no arg constructor */
  private class H2StorageNoArgs extends JDBCStorage("jdbc:h2:mem", 0)

}
