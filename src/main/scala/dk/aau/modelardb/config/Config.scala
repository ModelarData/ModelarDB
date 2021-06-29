package dk.aau.modelardb.config

import dk.aau.modelardb.core.{Correlation, Dimensions}
import dk.aau.modelardb.core.utility.{Pair, Static, ValueFunction}
import dk.aau.modelardb.engines.CodeGenerator
import dk.aau.modelardb.engines.spark.SparkProjector
import pureconfig.ConfigReader

import java.io.File
import java.nio.file.{FileSystems, Paths}
import java.util
import java.util.TimeZone
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

case class Config(
  modelarDb: ModelarConfig,
  arrow: ArrowConfig,
  mqtt: Option[MqttConfig]
)

case class ArrowConfig(
  flightPath: String,
  server: ArrowServerConfig,
  client: ArrowClientConfig
)

case class ArrowClientConfig(
  host: String,
  port: Int
)
case class ArrowServerConfig(
  host: String,
  port: Int
)

case class MqttConfig(
  host: String,
  port: Int,
  topic: String,
  clientId: String
)

case class ModelarConfig(
  engine: String,
  interface: String,
  batchSize: Int,
  sparkStreaming: Int,
  storage: String,
  source: String,
  derivedSource: List[String],
  dimensionSpec: Option[String],
  correlationSpec: Option[String],
  dynamicSplitFraction: Float,
  csv: CsvConfig,
  timestampColumn: Int,
  timezone: TimeZone,
  valueColumn: Int,
  models: Array[String],
  errorBound: Int,
  lengthBound: Int,
  maxLatency: Int,
  samplingInterval: Int,
  ingestors: Int,
  human: Boolean) {

  val sources: Array[String] = parseSource
  val derivedSources: java.util.HashMap[String, Array[Pair[String, ValueFunction]]] = deriveSources
  val derivedTimeSeries: java.util.HashMap[Integer, Array[Pair[String, ValueFunction]]] = new util.HashMap() // Is set by Partitioner
  var dimensions: Dimensions = dimensionSpec.fold
  { // The user have not specified any dimensions
    new Dimensions(Array())
  } { spec =>
    val lineSplit = spec.trim().split(" ", 2)
    readDimensionSpec(lineSplit(1))
  }

  var correlations: Array[Correlation] = correlationSpec.fold(Array.empty[Correlation])(parseCorrelations)

  private def parseSource: Array[String] = {
    // TODO: Replace with immutable collection
    val sources = ArrayBuffer.empty[String]
    val pathName = source
    val file = new File(pathName)
    if ((pathName.contains("*") && ! file.getParentFile.exists()) && ! file.exists()) {
      throw new IllegalArgumentException("ModelarDB: file/folder \"" + pathName + "\" do no exist")
    }

    file match {
      case _ if file.isFile => sources.append(pathName)
      case _ if file.isDirectory =>
        val files = file.listFiles.filterNot(_.isHidden).map(file => file.getAbsolutePath)
        sources.appendAll(files)
      case _ if file.getName.contains("*") =>
        //This is a simple glob-based filter that allows users to only ingest specific files, e.g., based on their suffix
        val folder = file.getParentFile
        val matcher = FileSystems.getDefault.getPathMatcher("glob:" + file.getName)
        val files = folder.listFiles.filterNot(_.isHidden).filter(file => matcher.matches(Paths.get(file.getName)))
        sources.appendAll(files.sorted.map(file => file.toString))
      //The path is not a file but contains a semicolon, so it is assumed to be an IP and port number
      case _ if pathName.contains(":") => sources.append(pathName)
      case _ =>
        throw new IllegalArgumentException("ModelarDB: source \"" + pathName + "\" in config file does not exist")
    }
    sources.toArray
  }

  private def deriveSources: java.util.HashMap[String, Array[Pair[String, ValueFunction]]] = {
    // TODO: Use idiomatic Scala and replace with immutable collection
    val derivedSources = new java.util.HashMap[String, ArrayBuffer[Pair[String, ValueFunction]]]()

    derivedSource.foreach{ source =>
      val derived = source.split(' ').map(_.trim)
      val sourceName = derived(0)
      val derivedName = derived(1)
      val functionName = derived(2)
      val transformation = CodeGenerator.getValueFunction(functionName) //HACK: reuses the engines dynamic codegen
      if(!derivedSources.containsKey(sourceName)) {
        derivedSources.put(derived(0), ArrayBuffer[Pair[String, ValueFunction]]())
      }
      derivedSources.get(derived(0)).append(new Pair(derivedName, transformation))
    }
    val result = new java.util.HashMap[String, Array[Pair[String, ValueFunction]]]()
    val iter = derivedSources.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      result.put(entry.getKey, entry.getValue.toArray)
    }
    result
  }

  private def parseCorrelations(str: String): Array[Correlation] = {
    //If the value is a file each line is considered a clause
    if (new File(str).exists()) {
      val correlationSource = Source.fromFile(str)
      val correlations = correlationSource.getLines.map { line =>
        parseCorrelation(line, dimensions)
      }
      correlationSource.close()
      correlations.toArray
    } else {
      Array(parseCorrelation(str, dimensions))
    }
  }

  private def parseCorrelation(line: String, dimensions: Dimensions): Correlation = {
    //The line is split into correlations and scaling factors
    var split = line.split('*')
    val correlations = split(0).split(',').map(_.trim)
    val scaling_factor = if (split.length > 1) split(1).split(',').map(_.trim) else Array[String]()
    val correlation = new Correlation()

    //Correlation is either specified as a set of sources, a set of LCA levels, a set of members, or a distance
    for (elem <- correlations) {
      split = elem.split(' ').map(_.trim)
      if (split.length == 1 && split(0).toLowerCase.equals("auto")) {
        //Automatic
        correlation.setDistance(dimensions.getLowestNoneZeroDistance)
      } else if (split.length == 1 && Static.isFloat(split(0))) {
        //Distance
        correlation.setDistance(split(0).toFloat)
      } else if (split.length == 2 && Static.isInteger(split(1).trim)) {
        //Dimensions and LCA level
        correlation.addDimensionAndLCA(split(0).trim, split(1).trim.toInt, dimensions)
      } else if (split.length >= 3 && Static.isInteger(split(1).trim)) {
        //Dimension, level, and members
        correlation.addDimensionAndMembers(split(0).trim, split(1).trim.toInt, split.drop(2), dimensions)
      } else {
        //Sources
        correlation.addSources(split)
      }
    }

    //Scaling factors are set for time series from specific sources, or for time series with specific members
    for (elem <- scaling_factor) {
      split = elem.split(' ')
      if (split.length == 2) {
        //Sets the scaling factor for time series from specific sources
        correlation.addScalingFactorForSource(split(0).trim, split(1).trim.toInt)
      } else if (split.length == 4) {
        //Sets the scaling factor for time series with a specific member
        correlation.addScalingFactorForMember(split(0).trim, split(1).trim.toInt, split(2).trim, split(3).trim.toFloat, dimensions)
      } else {
        throw new IllegalArgumentException("ModelarDB: unable to parse scaling factors \"" + elem + "\"")
      }
    }
    correlation
  }

  private def readDimensionSpec(dimensionPath: String): Dimensions = {
    Static.info(s"ModelarDB: $dimensionPath")
    //Checks if the user has specified a schema inline, and if not, ensures that the dimensions file exists
    if ( ! new File(dimensionPath).exists()) {
      val dimensions = dimensionPath.split(';').map(_.trim)
      if (dimensions(0).split(',').length < 2) {
        //A schema with a dimension that has no levels is invalid, so this must be a missing file
        throw new IllegalArgumentException("ModelarDB: file \"" + dimensionPath + "\" does no exist")
      }
      return new Dimensions(dimensions)
    }

    // Parses a dimensions file with the format (Dimension Definition+, Empty Line, Row+)
    val dimensionSource = Source.fromFile(dimensionPath)
    val lines = dimensionSource.getLines()
    var line: String = " "

    //Parses each dimension definition in the dimensions file
    val tables = mutable.Buffer[String]()
    while (lines.hasNext && line.nonEmpty) {
      line = lines.next().trim
      if (line.nonEmpty) {
        tables.append(line)
      }
    }
    val dimensions = new Dimensions(tables.toArray)

    //Skips the empty lines separating dimension definitions and rows
    lines.dropWhile(_.isEmpty)

    //Parses each row in the dimensions file
    line = " "
    while (lines.hasNext && line.nonEmpty) {
      line = lines.next().trim
      if (line.nonEmpty) {
        dimensions.add(line)
      }
    }
    dimensionSource.close()
    dimensions
  }

}

object ModelarConfig {
  // Defaults to GMT if input string is not a valid timezone ID
  implicit val timezoneReader: ConfigReader[TimeZone] = ConfigReader[String].map(TimeZone.getTimeZone)
}

case class DerivedSource(
  sorceName: String,
  derivedName: String,
  function: String
)

case class CsvConfig(
  separator: String,
  header: Boolean,
  dateformat: String,
  locale: String
)