package dk.aau.modelardb.arrow

import dk.aau.modelardb.core.Storage
import dk.aau.modelardb.engines.QueryEngine
import org.apache.arrow.flight.FlightProducer._
import org.apache.arrow.flight.{Action, ActionType, Criteria, FlightDescriptor, FlightInfo, FlightProducer, FlightStream, PutResult, Result, Ticket}

import java.nio.charset.StandardCharsets

class ArrowFlightProducer(queryEngine: QueryEngine, storage: Storage) extends FlightProducer {

  override def getStream(context: CallContext, ticket: Ticket, listener: ServerStreamListener): Unit = {
    val sql = new String(ticket.getBytes, StandardCharsets.UTF_8) // Assumes ticket is just a SQL query

    val root = queryEngine.execute(sql)
    listener.start(root)
    listener.putNext()
    listener.completed()
    root.close()
  }

  override def listFlights(context: CallContext, criteria: Criteria, listener: StreamListener[FlightInfo]): Unit = {
    listener.onError(???)
  }

  override def getFlightInfo(context: CallContext, descriptor: FlightDescriptor): FlightInfo = ???

  override def acceptPut(context: CallContext, flightStream: FlightStream, ackStream: StreamListener[PutResult]): Runnable = {
    () => {
      // TODO: How to check schema is for SegmentGroup?
      val flightRoot = flightStream.getRoot
      while (flightStream.next()) {
        ArrowUtil.storeData(storage, flightRoot)
      }
      flightRoot.close()
      flightStream.close()
    }
  }

  override def doAction(context: CallContext, action: Action, listener: StreamListener[Result]): Unit = {
    listener.onError(???)
  }

  override def listActions(context: CallContext, listener: StreamListener[ActionType]): Unit = {
    listener.onError(???)
  }
}
