package dk.aau.modelardb.arrow

import com.typesafe.scalalogging.Logger
import dk.aau.modelardb.core.Storage
import dk.aau.modelardb.engines.QueryEngine
import org.apache.arrow.flight.FlightProducer._
import org.apache.arrow.flight.impl.Flight
import org.apache.arrow.flight.{Action, ActionType, Criteria, FlightDescriptor, FlightInfo, FlightProducer, FlightStream, PutResult, Result, Ticket}

import java.nio.charset.StandardCharsets
import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.util.Try

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
    listener.onError(???)
  }

  override def getFlightInfo(context: CallContext, descriptor: FlightDescriptor): FlightInfo = ???

  override def acceptPut(context: CallContext, flightStream: FlightStream, ackStream: StreamListener[PutResult]): Runnable = {
        mode match {
          case "edge" =>
            () => { // doPut not accepted on edge nodes so just close the stream
              flightStream.close()
            }
          case "server" =>
            () => {
              // TODO: How to check schema is for SegmentGroup?
              // TODO: check that flightStream.getDescriptor.getPath matches a
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
    listener.onError(???)
  }

  override def listActions(context: CallContext, listener: StreamListener[ActionType]): Unit = {
    listener.onError(???)
  }
}
