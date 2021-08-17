package dk.aau.modelardb.akka

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.{ActorAttributes, OverflowStrategy, Supervision}
import akka.stream.alpakka.mqtt.streaming.{Command, Connect, ConnectFlags, ControlPacketFlags, MqttSessionSettings, Publish, Subscribe}
import akka.stream.alpakka.mqtt.streaming.scaladsl.{ActorMqttClientSession, Mqtt}
import akka.stream.scaladsl.{BroadcastHub, Keep, Sink, Source, SourceQueueWithComplete, Tcp}
import akka.util.ByteString
import com.typesafe.scalalogging.Logger
import dk.aau.modelardb.arrow.ArrowFlightClient
import dk.aau.modelardb.config.Config
import dk.aau.modelardb.core.{SegmentGroup, Storage}

import java.util.UUID
import scala.concurrent.Future
import scala.util.control.NonFatal

class AkkaSystem(config: Config, storage: Storage) {

  private val log = Logger(AkkaSystem.getClass)

  /* Akka */
  private implicit val actorSystem = ActorSystem(Behaviors.empty, "akka-streams")
  private implicit val ec = actorSystem.executionContext

  /* MQTT */
  private val mqttOpt = config.mqtt.map { mqttConfig =>
    val mqttTopic = mqttConfig.topic
    val settings = MqttSessionSettings()
    val mqttSession = ActorMqttClientSession(settings)
    val connection = Tcp()(actorSystem.classicSystem).outgoingConnection(mqttConfig.host, mqttConfig.port)
    val connectionId = UUID.randomUUID().toString
    val mqttClientFlow = Mqtt
      .clientSessionFlow(mqttSession, ByteString(connectionId))
      .join(connection)
    val mqttClient = Source
      .queue(2, OverflowStrategy.fail)
      .via(mqttClientFlow)
      .toMat(Sink.ignore)(Keep.left)
      .run()
    (mqttSession, mqttClient, mqttTopic)
  }

  /* Akka Stream */
  private val batchSize = config.modelarDb.batchSize
  private val numIngetors = if (config.modelarDb.ingestors == 0) { 1 } else { config.modelarDb.ingestors }

  private val arrowFlightClient = ArrowFlightClient(config.arrow)

  // Resume on non fatal error
  val decider: Supervision.Decider = {
    case NonFatal(ex) =>
      log.error("Got Exception", ex)
      Supervision.Stop
    case _ => Supervision.Stop
  }

  private val (queue, source) = Source.queue[SegmentGroup](batchSize * numIngetors, OverflowStrategy.dropHead, numIngetors)
    .toMat(BroadcastHub.sink(2048))(Keep.both)
    .withAttributes(ActorAttributes.supervisionStrategy(decider))
    .run()

  private val arrowSink = Sink.foreach[Seq[SegmentGroup]](arrowFlightClient.doPut)
  private val jdbcSink = Sink.foreach[Seq[SegmentGroup]](sgs => storage.storeSegmentGroups(sgs.toArray, batchSize))
  private val mqttSink = mqttOpt.map { case (mqttSession, _, mqttTopic) =>
    Sink.foreach[SegmentGroup] { sg =>
      val byteString = ByteString(sg.toString.getBytes) // TODO: what format should we use for deserialization
      mqttSession ! Command(Publish(ControlPacketFlags.QoSAtMostOnceDelivery, mqttTopic, byteString))
    }
  }

  def start: SourceQueueWithComplete[SegmentGroup] = {
    (mqttOpt, mqttSink) match {
      case (None, _) | (_, None) =>
      case (Some((_, mqttClient, mqttTopic)), Some(mqttSink)) =>
          val clientId = config.mqtt.map(_.clientId)
            .getOrElse {
              val msg = "MQTT client ID not defined"
              log.error(msg)
              throw new Exception(msg)
            }
          mqttClient.offer(Command(Connect(clientId, ConnectFlags.None)))
          mqttClient.offer(Command(Subscribe(mqttTopic)))
          source.to(mqttSink).run()
    }
    source.grouped(batchSize).to(arrowSink).run()
    source.grouped(batchSize).to(jdbcSink).run()
    queue
  }

  def stop(): Unit = {
    val onComplete = mqttOpt match {
      case Some((mqttSession, mqttClient, _)) =>
        mqttClient.complete()
        mqttClient.watchCompletion.map { _ => mqttSession.shutdown() }
      case None => Future.unit
    }
    onComplete.andThen {
      case _ => actorSystem.terminate()
    }
  }

}

object AkkaSystem {
  def apply(config: Config, storage: Storage): AkkaSystem = new AkkaSystem(config, storage)
}
