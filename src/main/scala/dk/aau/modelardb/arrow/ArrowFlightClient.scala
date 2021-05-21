package dk.aau.modelardb.arrow

import dk.aau.modelardb.config.{ArrowConfig, Config}
import dk.aau.modelardb.core.SegmentGroup
import io.grpc.ManagedChannelBuilder
import org.apache.arrow.flight.FlightClient.PutListener
import org.apache.arrow.flight.{AsyncPutListener, FlightDescriptor, FlightGrpcUtils, PutResult, SyncPutListener, Ticket}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot

import java.nio.charset.StandardCharsets

class ArrowFlightClient(config: ArrowConfig) {

  val channel = ManagedChannelBuilder
    .forAddress(config.host, config.port)
    .usePlaintext()
    .build()

  val allocator = new RootAllocator()
  val client = FlightGrpcUtils.createFlightClient(allocator, channel)
  val root = VectorSchemaRoot.create(SegmentGroupSchema.arrowSchema, allocator)
  val flightDescriptor = FlightDescriptor.path(config.flightPath)

  val metadataListener = new AsyncPutListener {
    override def onNext(result: PutResult): Unit = {
      println("received message from server: ", result)
      result.close()
    }
  }


  def doPut(segmentGroups: Seq[SegmentGroup]): Seq[SegmentGroup] = {
    val streamListener = client.startPut(flightDescriptor, root, metadataListener)
    segmentGroups.zipWithIndex.foreach { case (sg, index) =>
      ArrowUtil.addToRoot(index, sg, root)
    }
    root.setRowCount(segmentGroups.size)
    streamListener.putNext()
    while (!streamListener.isReady) {}
    streamListener.completed()
    streamListener.getResult()
    root.clear()
    segmentGroups
  }

}

object ArrowFlightClient {

  def apply(config: ArrowConfig): ArrowFlightClient = new ArrowFlightClient(config)

  def main(args: Array[String]): Unit = {

    val channel = ManagedChannelBuilder
      .forAddress("localhost", 6006)
      .usePlaintext()
      .build()

    val allocator = new RootAllocator()
    val client = FlightGrpcUtils.createFlightClient(allocator, channel)

//    val ticket = new Ticket("select * from segment limit 10".getBytes(StandardCharsets.UTF_8))
//    val stream = client.getStream(ticket)
//    while(stream.next()) {
//      val result = stream.getRoot.contentToTSVString()
//      println(s"got result:")
//      println(result)
//    }
//    stream.close()

    val desc = FlightDescriptor.path("sensor/1")
    val root = ArrowUtil.insertTestSGData(10,
      VectorSchemaRoot.create(SegmentGroupSchema.arrowSchema, allocator)
    )

    val metadataListener = new AsyncPutListener {
      override def onNext(result: PutResult): Unit = {
        println("received message from server: ", result)
        result.close()
      }
    }

    val streamListener = client.startPut(desc, root, metadataListener)
    streamListener.putNext()
    streamListener.completed()
    streamListener.getResult()
  }

}
