package dk.aau.modelardb.arrow

import dk.aau.modelardb.config.ArrowConfig
import dk.aau.modelardb.core.{SegmentGroup, TimeSeriesGroup}
import io.grpc.ManagedChannelBuilder
import org.apache.arrow.flight.{Action, AsyncPutListener, FlightDescriptor, FlightGrpcUtils, PutResult}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot

import scala.collection.JavaConverters._
import java.nio.charset.StandardCharsets.UTF_8
import scala.util.{Failure, Success, Try}

class ArrowFlightClient(config: ArrowConfig) {

  val edgeId = config.client.edgeId

  val channel = ManagedChannelBuilder
    .forAddress(config.client.host, config.client.port)
    .usePlaintext()
    .build()

  val allocator = new RootAllocator()
  val client = FlightGrpcUtils.createFlightClient(allocator, channel)
  val segmentRoot = VectorSchemaRoot.create(SegmentSchema.toArrow, allocator)
  val timeseriesRoot = VectorSchemaRoot.create(TimeseriesSchema.toArrow, allocator)
  val flightDescriptor = FlightDescriptor.path(config.flightPath)

  val metadataListener = new AsyncPutListener {
    override def onNext(result: PutResult): Unit = {
      println(s"received message from server: $result")
      result.close()
    }
  }

  def putTimeseries(timeseriesGroup: Seq[TimeSeriesGroup]): Seq[TimeSeriesGroup] = {
    val streamListener = client.startPut(flightDescriptor, timeseriesRoot, metadataListener)
    var count = 0;
    timeseriesGroup.zipWithIndex.foreach{ case (tsg, index) =>
      tsg.getTimeSeries.foreach { ts =>
        ArrowUtil.addTsToRoot(index, tsg.gid, ts, timeseriesRoot)
        count += 1
      }
    }
    timeseriesRoot.setRowCount(count)
    streamListener.putNext()
    while (!streamListener.isReady) {}
    streamListener.completed()
    streamListener.getResult()
    timeseriesRoot.clear()
    timeseriesGroup
  }

  def putSegmentGroups(segmentGroups: Seq[SegmentGroup]): Seq[SegmentGroup] = {
    val streamListener = client.startPut(flightDescriptor, segmentRoot, metadataListener)
    segmentGroups.zipWithIndex.foreach { case (sg, index) =>
      ArrowUtil.addSegmentToRoot(index, sg, segmentRoot)
    }
    segmentRoot.setRowCount(segmentGroups.size)
    streamListener.putNext()
    while (!streamListener.isReady) {}
    streamListener.completed()
    streamListener.getResult()
    segmentRoot.clear()
    segmentGroups
  }

  def getTidOffset(tsCount: Int): Int = {
    val body = s"$edgeId,$tsCount".getBytes(UTF_8)
    val action = new Action("TID", body)
    val results = client.doAction(action)
    val result = if (results.hasNext) {
      val bytes = results.next().getBody
      Try(new String(bytes, UTF_8).toInt)
    } else {
      throw new Exception("Unable to obtain TID offset from server")
    }
    result match {
      case Failure(exception) => throw exception
      case Success(value) => value
    }
  }

  def getGidOffset(gidCount: Int): Int = {
    val body = s"$edgeId,$gidCount".getBytes(UTF_8)
    val action = new Action("GID", body)
    val results = client.doAction(action)
    val result = if (results.hasNext) {
      val bytes = results.next().getBody
      Try(new String(bytes, UTF_8).toInt)
    } else {
      throw new Exception("Unable to obtain GID offset from server")
    }
    result match {
      case Failure(exception) => throw exception
      case Success(value) => value
    }
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

    val desc = FlightDescriptor.path("sensor/1")
    val root = ArrowUtil.insertTestSGData(10,
      VectorSchemaRoot.create(SegmentSchema.toArrow, allocator)
    )

    val metadataListener = new AsyncPutListener {
      override def onNext(result: PutResult): Unit = {
        println(s"received message from server: $result")
        result.close()
      }
    }

    val streamListener = client.startPut(desc, root, metadataListener)
    streamListener.putNext()
    streamListener.completed()
    streamListener.getResult()
  }

}
