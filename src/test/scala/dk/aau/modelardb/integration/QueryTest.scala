/* Copyright 2021 The ModelarDB Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dk.aau.modelardb.integration

import dk.aau.modelardb.core.{Configuration, Dimensions}
import dk.aau.modelardb.storage.StorageFactory
import dk.aau.modelardb.engines.h2.H2
import dk.aau.modelardb.engines.h2.H2Storage
import dk.aau.modelardb.core.Correlation
import dk.aau.modelardb.engines.spark.Spark
import dk.aau.modelardb.engines.spark.SparkStorage

import java.lang.reflect.Field
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.time.Duration
import java.util
import java.util.concurrent.Executors
import scala.collection.mutable
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.ConfigFactory
import dk.aau.modelardb.config.{Config, ModelarConfig}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pureconfig.{ConfigObjectSource, ConfigSource}
import pureconfig.generic.auto._
import scala.collection.JavaConverters._

class QueryTest extends AnyFlatSpec with Matchers {

  val defaultConfig = ConfigSource.resources("test.conf")

  /** Instance Variable **/
  private val h2Port = 9991
  private var h2Engine: H2 = null
  private val sparkPort = 9992
  private var sparkEngine: Spark = null
  private var sparkStoragefield: Field = null
  private val storageDirectory = Files.createTempDirectory("").toFile
  private val storages = List(
    StorageFactory.getStorage("jdbc:h2:" + storageDirectory + "/h2", 0),
    StorageFactory.getStorage("orc:" + storageDirectory + "/orc", 0),
    StorageFactory.getStorage("parquet:" + storageDirectory + "/parquet", 0)
  )

  //Ingest
  val executor = Executors.newCachedThreadPool()
  "ModelarDB" should "support ingesting test data using H2" in new TimeSeriesGroupProvider {
    val configValues = Map(
      "modelardb.sources" -> getTimeSeriesFiles.map(_.getAbsolutePath()),
      "modelardb.model_types" -> modelTypeNames,
      "modelardb.sampling_interval" -> getSamplingInterval
    )
    val config = createConfig(configValues)
    for (storage <- storages) {
      val h2Engine = new H2(config.modelarDb, storage.asInstanceOf[H2Storage], arrowFlightClient = null)
      h2Engine.start()
    }
  }

  //Connect
  it should "initialize the H2 and Apache Spark-based query engines" in {
    assume(TimeSeriesGroupProvider.testDataWasProvided, TimeSeriesGroupProvider.missingTestDataMessage)
    val h2ConfigValues = Map(
      "modelardb.model_types" -> Array(),
      "modelardb.sampling_interval" -> -1,
      "modelardb.interface" -> s"http:$h2Port"
    )
    val h2config = createConfig(h2ConfigValues)
    this.h2Engine = new H2(h2config.modelarDb, storages(0).asInstanceOf[H2Storage], arrowFlightClient = null)
    executor.execute(() => h2Engine.start())

    val sparkConfigValues =  Map(
      "modelardb.model_types" -> Array(),
      "modelardb.sampling_interval" -> -1,
      "modelardb.interface" -> s"http:$sparkPort",
      "modelardb.engine" -> "spark"
    )
    val sparkConfiguration = createConfig(sparkConfigValues)
    this.sparkEngine = new Spark(sparkConfiguration.modelarDb, storages(0).asInstanceOf[SparkStorage], arrowFlightClient = null)
    this.sparkStoragefield = sparkEngine.getClass.getDeclaredField("sparkStorage")
    this.sparkStoragefield.setAccessible(true)
    executor.execute(() => sparkEngine.start())
    Thread.sleep(10000) //HACK: wait for the query interfaces to be initialized
  }

  //Query
  //No Response
  it should "return nothing when requesting missing data from Segment and DataPoint" in {
    assume(TimeSeriesGroupProvider.testDataWasProvided, TimeSeriesGroupProvider.missingTestDataMessage)
    assertEnginesAndStorageAreEquivalent("SELECT * FROM Segment WHERE tid = -1",
      "SELECT * FROM DataPoint WHERE tid = -1")
  }

  //Aggregates Over Data Set
  it should "return same result for COUNT from Segment and DataPoint" in {
    assume(TimeSeriesGroupProvider.testDataWasProvided, TimeSeriesGroupProvider.missingTestDataMessage)
    assertEnginesAndStorageAreEquivalent("SELECT COUNT_S(#) FROM Segment",
      "SELECT COUNT(*) FROM DataPoint", "SELECT COUNT(value) FROM DataPoint")
  }

  it should "return same result for MIN from Segment and DataPoint" in {
    assume(TimeSeriesGroupProvider.testDataWasProvided, TimeSeriesGroupProvider.missingTestDataMessage)
    assertEnginesAndStorageAreEquivalent("SELECT MIN_S(#) FROM Segment",
      "SELECT MIN(value) FROM DataPoint")
  }

  it should "return same result for MAX from Segment and DataPoint" in {
    assume(TimeSeriesGroupProvider.testDataWasProvided, TimeSeriesGroupProvider.missingTestDataMessage)
    assertEnginesAndStorageAreEquivalent("SELECT MAX_S(#) FROM Segment",
      "SELECT MAX(value) FROM DataPoint")
  }

  it should "return same result for SUM from Segment and DataPoint" in {
    assume(TimeSeriesGroupProvider.testDataWasProvided, TimeSeriesGroupProvider.missingTestDataMessage)
    assertEnginesAndStorageAreEquivalent("SELECT SUM_S(#) FROM Segment",
      "SELECT SUM(value) FROM DataPoint")
  }

  it should "return same result for AVG from Segment and DataPoint" in {
    assume(TimeSeriesGroupProvider.testDataWasProvided, TimeSeriesGroupProvider.missingTestDataMessage)
    assertEnginesAndStorageAreEquivalent("SELECT AVG_S(#) FROM Segment",
      "SELECT AVG(value) FROM DataPoint")
  }

  //Aggregates Over Series
  it should "return same result for COUNT from Segment and DataPoint WHERE tid = 1" in {
    assume(TimeSeriesGroupProvider.testDataWasProvided, TimeSeriesGroupProvider.missingTestDataMessage)
    assertEnginesAndStorageAreEquivalent("SELECT COUNT_S(#) FROM Segment WHERE tid = 1",
      "SELECT COUNT(*) FROM DataPoint WHERE tid = 1",
      "SELECT COUNT(value) FROM DataPoint WHERE tid = 1")
  }

  it should "return same result for MIN from Segment and DataPoint WHERE tid = 1" in {
    assume(TimeSeriesGroupProvider.testDataWasProvided, TimeSeriesGroupProvider.missingTestDataMessage)
    assertEnginesAndStorageAreEquivalent("SELECT MIN_S(#) FROM Segment WHERE tid = 1",
      "SELECT MIN(value) FROM DataPoint WHERE tid = 1")
  }

  it should "return same result for MAX from Segment and DataPoint WHERE tid = 1" in {
    assume(TimeSeriesGroupProvider.testDataWasProvided, TimeSeriesGroupProvider.missingTestDataMessage)
    assertEnginesAndStorageAreEquivalent("SELECT MAX_S(#) FROM Segment WHERE tid = 1",
      "SELECT MAX(value) FROM DataPoint WHERE tid = 1")
  }

  it should "return same result for SUM from Segment and DataPoint WHERE tid = 1" in {
    assume(TimeSeriesGroupProvider.testDataWasProvided, TimeSeriesGroupProvider.missingTestDataMessage)
    assertEnginesAndStorageAreEquivalent("SELECT SUM_S(#) FROM Segment WHERE tid = 1",
      "SELECT SUM(value) FROM DataPoint WHERE tid = 1")
  }

  it should "return same result for AVG from Segment and DataPoint WHERE tid = 1" in {
    assume(TimeSeriesGroupProvider.testDataWasProvided, TimeSeriesGroupProvider.missingTestDataMessage)
    assertEnginesAndStorageAreEquivalent("SELECT AVG_S(#) FROM Segment WHERE tid = 1",
      "SELECT AVG(value) FROM DataPoint WHERE tid = 1")
  }

  //Point and Range Queries
  it should "return same result from DataPoint WHERE tid = 1 ORDER BY timestamp LIMIT 10" in {
    assume(TimeSeriesGroupProvider.testDataWasProvided, TimeSeriesGroupProvider.missingTestDataMessage)
    assertEnginesAndStorageAreEquivalent(
      "SELECT tid, timestamp, value FROM DataPoint WHERE tid = 1 ORDER BY timestamp LIMIT 10")
  }

  //Cleanup
  it should "delete the folder containing the all of the ingested data" in {
    assume(TimeSeriesGroupProvider.testDataWasProvided, TimeSeriesGroupProvider.missingTestDataMessage)
    new scala.reflect.io.Directory(this.storageDirectory).deleteRecursively()
  }


  /** Private Methods **/

  private def createConfig(properties: Map[String, Any]): Config = {
    import ModelarConfig.timezoneReader // needed to read config
    val localConfig = ConfigFactory.parseMap(properties.asJava)
    ConfigSource
      .fromConfig(localConfig)
      .withFallback(defaultConfig)
      .loadOrThrow[Config]
  }

  private def assertEnginesAndStorageAreEquivalent(queries: String *) = {
    //Ensure that test data is available before executing queries

    //Execute queries
    val results = mutable.ArrayBuffer[List[Map[String, Object]]]()
    for (storage <- storages) {
      H2.initialize(h2Engine, storage.asInstanceOf[H2Storage])
      this.sparkStoragefield.set(sparkEngine, storage.asInstanceOf[SparkStorage])
      for (query <- queries) {
        results.append(executeQuery(query, h2Port))
        results.append(executeQuery(query, sparkPort))
      }
    }

    //Compare the size of all result sets
    val sizeOfFirstResult = results(0).size
    assert(results.forall(result => result.size === sizeOfFirstResult))

    //Compare the contents of all result sets
    val sizeOfResults = results.size
    for (rowIndex <- Range(0, sizeOfFirstResult)) {
      val firstResultRow = results(0)(rowIndex).values
      for (resultIndex <- Range(1, sizeOfResults)) {
        val currentResultRow = results(resultIndex)(rowIndex).values
        for (fc <- firstResultRow.zip(currentResultRow)) {
             isEqualEnough(fc._1, fc._2)
        }
      }
    }
  }

  private def executeQuery(query: String, port: Int): List[Map[String, Object]] = {
    val client = HttpClient.newBuilder().build()
    val request = HttpRequest.newBuilder()
      .uri(URI.create("http://127.0.0.1:" + port))
      .timeout(Duration.ofMinutes(1))
      .POST(HttpRequest.BodyPublishers.ofString(query))
      .build()
    val response = client.send(request, HttpResponse.BodyHandlers.ofString())

    val mapper = new ObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val result = mapper.readValue(response.body, classOf[Map[String, Object]])
    result("result").asInstanceOf[List[Map[String,Object]]]
  }

  private def isEqualEnough(v1: Object, v2: Object): Unit = {
    //Both are integers
    if ((v1.isInstanceOf[Int] && v2.isInstanceOf[Int]) ||
      (v1.isInstanceOf[String] && v2.isInstanceOf[String])) {
      assert(v1 === v2)
    } else if (v1.isInstanceOf[Double] && v2.isInstanceOf[Double]) {
      //Values are provided with the accuracy of 32-bit floats but parsed to doubles
      assert(v1.asInstanceOf[Double].floatValue === v2.asInstanceOf[Double].floatValue)
    } else {
      throw new IllegalArgumentException(v1.getClass + " " + v2.getClass)
    }
  }
}
