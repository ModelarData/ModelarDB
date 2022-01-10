package dk.aau.modelardb.arrow

import com.typesafe.scalalogging.Logger
import dk.aau.modelardb.OffsetType
import dk.aau.modelardb.engines.QueryEngine
import dk.aau.modelardb.storage.Storage
import org.apache.arrow.flight.FlightProducer._
import org.apache.arrow.flight.{Action, ActionType, Criteria, FlightDescriptor, FlightInfo, FlightProducer, FlightStream, PutResult, Result, Ticket}

import java.nio.charset.StandardCharsets.UTF_8
import scala.util.{Failure, Success, Try}

class ArrowFlightProducer(queryEngine: QueryEngine, storage: Storage, mode: String) extends FlightProducer {

  val log = Logger(this.getClass)

  override def getStream(context: CallContext, ticket: Ticket, listener: ServerStreamListener): Unit = {
    val sql = new String(ticket.getBytes, UTF_8) // Assumes ticket is just a SQL query

    val root = queryEngine.execute(sql)
    listener.start(root)
    listener.putNext()
    listener.completed()
    root.close()
  }

  override def listFlights(context: CallContext, criteria: Criteria, listener: StreamListener[FlightInfo]): Unit = {
    ???
  }

  override def getFlightInfo(context: CallContext, descriptor: FlightDescriptor): FlightInfo = {
    ???
  }

  override def acceptPut(context: CallContext, flightStream: FlightStream, ackStream: StreamListener[PutResult]): Runnable = {
    mode match {
      case "edge" =>
        () => { // doPut not accepted on edge nodes so just close the stream
          flightStream.close()
        }
      case "server" =>
        () => {
          val flightRoot = flightStream.getRoot
          while (flightStream.next()) {
            ArrowUtil.storeData(storage, flightRoot)
          }
          flightRoot.close()
          flightStream.close()
        }
    }
  }

  override def doAction(context: CallContext, action: Action, listener: StreamListener[Result]): Unit = {
    if (mode.equalsIgnoreCase("edge")) {
      listener.onError(new Exception("doAction not supported on edge nodes"))
      return
    }

    val actionBody = new String(action.getBody, UTF_8)
    val parameters = actionBody.split(",") // parameters = [edgeId, timeseriesCount]
    val edgeId = parameters(0)
    val count = Try[Int](parameters(1).toInt) match {
      case Failure(exception) =>
        listener.onError(exception)
        return
      case Success(value) => value
    }
    val result: Int = action.getType.toUpperCase match {
      case "TID" => storage.getOffset(OffsetType.TID, edgeId, count)
      case "GID" => storage.getOffset(OffsetType.GID, edgeId, count)
      case _ @ unknown =>
        listener.onError(new Exception(s"Unknown Action Type: ${unknown}"))
        return
    }
    val resultBody = result.toString.getBytes(UTF_8)
    listener.onNext(new Result(resultBody))
    listener.onCompleted()
  }

  override def listActions(context: CallContext, listener: StreamListener[ActionType]): Unit = {
    listener.onNext(new ActionType("TID", "The next TID for the edge to use for its Timeseries"))
    listener.onNext(new ActionType("GID", "The next GID for the edge to use for its Segment Groups"))
    listener.onCompleted()
  }
}
