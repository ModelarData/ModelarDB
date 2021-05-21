package dk.aau.modelardb.akka

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.{ActorAttributes, OverflowStrategy, Supervision, SystemMaterializer}
import akka.stream.alpakka.mqtt.streaming.{Command, Connect, ConnectFlags, ControlPacketFlags, MqttSessionSettings, Publish, Subscribe}
import akka.stream.alpakka.mqtt.streaming.scaladsl.{ActorMqttClientSession, Mqtt}
import akka.stream.scaladsl.{BroadcastHub, Keep, Sink, Source, SourceQueueWithComplete, Tcp}
import akka.util.ByteString
import dk.aau.modelardb.Main.log
import dk.aau.modelardb.arrow.ArrowFlightClient
import dk.aau.modelardb.config.Config
import dk.aau.modelardb.core.{SegmentGroup, Storage}

import java.util.UUID
import scala.util.control.NonFatal

class AkkaSystem(config: Config, storage: Storage) {

  /* Akka */
  implicit val actorSystem = ActorSystem(Behaviors.empty, "akka-streams")
  private implicit val ec = actorSystem.executionContext
//  private implicit val materializer = SystemMaterializer(actorSystem).materializer

  /* MQTT */
  private val mqttTopic = config.mqtt.topic
  private val settings = MqttSessionSettings()
  private val mqttSession = ActorMqttClientSession(settings)
  private val connection = Tcp()(actorSystem.classicSystem).outgoingConnection(config.mqtt.host, config.mqtt.port)
  private val connectionId = UUID.randomUUID().toString
  private val mqttClientFlow = Mqtt
    .clientSessionFlow(mqttSession, ByteString(connectionId))
    .join(connection)
  private val mqttClient = Source
    .queue(2, OverflowStrategy.fail)
    .via(mqttClientFlow)
    .toMat(Sink.ignore)(Keep.left)
    .run()

  /* Akka Stream */
  private val batchSize = config.modelarDb.batchSize
  private val numIngetors = config.modelarDb.ingestors

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
  private val mqttSink = Sink.foreach[SegmentGroup] { sg =>
    val byteString = ByteString(sg.toString.getBytes) // TODO: what format should we use for deserialization
    mqttSession ! Command(Publish(ControlPacketFlags.QoSAtMostOnceDelivery, mqttTopic, byteString))
  }

  def start: SourceQueueWithComplete[SegmentGroup] = {
    mqttClient.offer(Command(Connect(config.mqtt.clientId, ConnectFlags.None)))
    mqttClient.offer(Command(Subscribe(mqttTopic)))
    source.grouped(batchSize).to(arrowSink).run()
    source.grouped(batchSize).to(jdbcSink).run()
    source.to(mqttSink).run()
    queue
  }

  def stop(): Unit = {
    mqttClient.complete()
    mqttClient.watchCompletion.foreach { _ =>
      mqttSession.shutdown()
      actorSystem.terminate()
    }
  }

}

object AkkaSystem {
  def apply(config: Config, storage: Storage): AkkaSystem = new AkkaSystem(config, storage)
}
