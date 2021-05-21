package dk.aau.modelardb.config

import pureconfig.ConfigReader

import java.util.TimeZone

case class Config(
  modelarDb: ModelarConfig,
  arrow: ArrowConfig,
  mqtt: MqttConfig
)

case class ModelarConfig(
  engine: String,
  interface: String,
  batchSize: Int,
  sparkStreaming: Int,
  storage: String,
  source: String,
  dimensions: Option[String],
  correlation: Option[String],
  dynamicSplitFraction: Float,
  csv: CsvConfig,
  timestampColumn: Int,
  timezone: TimeZone,
  valueColumn: Int,
  models: List[String],
  errorBound: Int,
  lengthBound: Int,
  maxLatency: Int,
  samplingInterval: Int,
  ingestors: Int,
  human: Boolean
)

case class CsvConfig(
  separator: String,
  header: Boolean,
  dateformat: String,
  locale: String
)

case class ArrowConfig(
  host: String,
  port: Int,
  flightPath: String
)

case class MqttConfig(
  host: String,
  port: Int,
  topic: String,
  clientId: String
)

object Config {
  import pureconfig.configurable._

  // Defaults to GMT if input string is not a valid timezone ID
  implicit val timezoneReader = ConfigReader[String].map(TimeZone.getTimeZone)
}
