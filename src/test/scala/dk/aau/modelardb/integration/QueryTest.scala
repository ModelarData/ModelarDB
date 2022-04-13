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

import java.io.{OutputStream, PrintStream}
import java.lang.reflect.Field
import java.nio.file.Files
import java.util
import java.util.TimeZone
import java.util.concurrent.Executors

import scala.collection.{SortedMap, mutable}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

abstract class QueryTest extends AnyFlatSpec with Matchers {

  //HACK: Disable stdout during testing as some of the tests are very noisy
  System.setOut(new PrintStream(OutputStream.nullOutputStream()))
  System.setErr(new PrintStream(OutputStream.nullOutputStream()))

  //Ensures the tests use the same time zone independent of the system's setting
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  /** Instance Variables **/
  private val (h2Port, sparkPort) = this.getPorts
  private var h2Engine: H2 = null
  private var sparkEngine: Spark = null
  private var sparkStorageField: Field = null
  private val storageDirectory = Files.createTempDirectory("").toFile
  private val storages = List(
    StorageFactory.getStorage("jdbc:h2:" + this.storageDirectory + "/h2"),
    StorageFactory.getStorage("orc:" + this.storageDirectory + "/orc"),
    StorageFactory.getStorage("parquet:" + this.storageDirectory + "/parquet")
  )
  private val modelTypeNames = Array(
    "dk.aau.modelardb.core.models.PMC_MeanModelType",
    "dk.aau.modelardb.core.models.SwingFilterModelType",
    "dk.aau.modelardb.core.models.FacebookGorillaModelType"
  )

  //Ingest
  "ModelarDB" should "support ingesting test data using H2" in new TimeSeriesGroupProvider {
    val configuration = createConfiguration(Map(
      "modelardb.ingestors" -> 1,
      "modelardb.sources" -> getTimeSeriesFiles.map(_.getAbsolutePath()),
      "modelardb.model_types" -> modelTypeNames,
      "modelardb.sampling_interval" -> getSamplingInterval
    ))

    for (storage <- storages) {
      val h2Engine = new H2(configuration, storage.asInstanceOf[H2Storage])
      h2Engine.start()
    }
  }

  //Connect
  it should "initialize the H2 and Apache Spark-based query engines" in {
    val h2ConfigurationValues = Map(
      "modelardb.model_types" -> Array(),
      "modelardb.sampling_interval" -> -1,
      "modelardb.interface" -> this.getInterface(this.h2Port)
    )
    val h2configuration = createConfiguration(h2ConfigurationValues)
    this.h2Engine = new H2(h2configuration, storages(0).asInstanceOf[H2Storage])
    h2configuration.getExecutorService().execute(() => h2Engine.start())

    val sparkConfigurationValues =  Map(
      "modelardb.model_types" -> Array(),
      "modelardb.sampling_interval" -> -1,
      "modelardb.interface" -> this.getInterface(this.sparkPort),
      "modelardb.engine" -> "spark"
    )
    val sparkConfiguration = createConfiguration(sparkConfigurationValues)
    this.sparkEngine = new Spark(sparkConfiguration, storages(0).asInstanceOf[SparkStorage])
    this.sparkStorageField = sparkEngine.getClass.getDeclaredField("sparkStorage")
    this.sparkStorageField.setAccessible(true)
    sparkConfiguration.getExecutorService().execute(() => sparkEngine.start())
    Thread.sleep(10000) //HACK: wait for the query interfaces to be initialized
  }

  //Query
  //No Response
  it should "return nothing when requesting missing data from Segment and DataPoint" in {
    assertEnginesAndStorageAreEquivalent("SELECT * FROM Segment WHERE tid = -1",
      "SELECT * FROM DataPoint WHERE tid = -1")
  }

  //Aggregates Over Entire Data Set
  it should "return same result for COUNT from Segment and DataPoint" in {
    assertEnginesAndStorageAreEquivalent("SELECT COUNT_S(#) FROM Segment",
      "SELECT COUNT(*) FROM DataPoint", "SELECT COUNT(value) FROM DataPoint")
  }

  it should "return same result for MIN from Segment and DataPoint" in {
    assertEnginesAndStorageAreEquivalent("SELECT MIN_S(#) FROM Segment",
      "SELECT MIN(value) FROM DataPoint")
  }

  it should "return same result for MAX from Segment and DataPoint" in {
    assertEnginesAndStorageAreEquivalent("SELECT MAX_S(#) FROM Segment",
      "SELECT MAX(value) FROM DataPoint")
  }

  it should "return same result for SUM from Segment and DataPoint" in {
    assertEnginesAndStorageAreEquivalent("SELECT SUM_S(#) FROM Segment",
      "SELECT SUM(value) FROM DataPoint")
  }

  it should "return same result for AVG from Segment and DataPoint" in {
    assertEnginesAndStorageAreEquivalent("SELECT AVG_S(#) FROM Segment",
      "SELECT AVG(value) FROM DataPoint")
  }

  //Aggregates Over Individual Time Series
  it should "return same result for COUNT from Segment and DataPoint WHERE tid = 1" in {
    assertEnginesAndStorageAreEquivalent("SELECT COUNT_S(#) FROM Segment WHERE tid = 1",
      "SELECT COUNT(*) FROM DataPoint WHERE tid = 1",
      "SELECT COUNT(value) FROM DataPoint WHERE tid = 1")
  }

  it should "return same result for MIN from Segment and DataPoint WHERE tid = 1" in {
    assertEnginesAndStorageAreEquivalent("SELECT MIN_S(#) FROM Segment WHERE tid = 1",
      "SELECT MIN(value) FROM DataPoint WHERE tid = 1")
  }

  it should "return same result for MAX from Segment and DataPoint WHERE tid = 1" in {
    assertEnginesAndStorageAreEquivalent("SELECT MAX_S(#) FROM Segment WHERE tid = 1",
      "SELECT MAX(value) FROM DataPoint WHERE tid = 1")
  }

  it should "return same result for SUM from Segment and DataPoint WHERE tid = 1" in {
    assertEnginesAndStorageAreEquivalent("SELECT SUM_S(#) FROM Segment WHERE tid = 1",
      "SELECT SUM(value) FROM DataPoint WHERE tid = 1")
  }

  it should "return same result for AVG from Segment and DataPoint WHERE tid = 1" in {
    assertEnginesAndStorageAreEquivalent("SELECT AVG_S(#) FROM Segment WHERE tid = 1",
      "SELECT AVG(value) FROM DataPoint WHERE tid = 1")
  }

  //Point and Range Queries
  it should "support querying all columns from Segment with LIMIT 10" in {
    executeQueries("SELECT * FROM Segment LIMIT 10")
  }

  it should "support querying all columns from DataPoint with LIMIT 10" in {
    executeQueries("SELECT * FROM DataPoint LIMIT 10")
  }

  it should "return same result from DataPoint WHERE tid = 1 ORDER BY timestamp LIMIT 10" in {
    assertEnginesAndStorageAreEquivalent(
      "SELECT tid, timestamp, value FROM DataPoint WHERE tid = 1 ORDER BY timestamp LIMIT 10")
  }

  //Cleanup
  it should "delete the folder containing the all of the ingested data" in {
    new scala.reflect.io.Directory(this.storageDirectory).deleteRecursively()
  }

  /** Protected Methods **/
  protected def getPorts: (Int, Int)
  protected def getInterface(port: Int): String
  protected def executeQuery(query: String, port: Int): List[SortedMap[String, Object]]

  /** Private Methods **/
  private def createConfiguration(configurationValues: Map[String, Any]): Configuration = {
    val configuration = new Configuration()

    //Shared configuration
    configuration.add("modelardb.batch_size", 50000);
    configuration.add("modelardb.dimensions", new Dimensions(Array()));
    configuration.add("modelardb.sources.derived", new util.HashMap());
    configuration.add("modelardb.correlations", Array[Correlation]());
    configuration.add("modelardb.dynamic_split_fraction", 10);
    configuration.add("modelardb.executor_service", Executors.newCachedThreadPool());
    configuration.add("modelardb.timestamp_column", 0);
    configuration.add("modelardb.value_column", 1);
    configuration.add("modelardb.time_zone", "UTC");
    configuration.add("modelardb.csv.date_format", "unix");
    configuration.add("modelardb.csv.header", false);
    configuration.add("modelardb.csv.locale", "en");
    configuration.add("modelardb.csv.separator", "");

    //Test configuration
    for ((name, value) <- configurationValues) {
      configuration.add(name, value)
    }
    configuration
  }

  private def assertEnginesAndStorageAreEquivalent(queries: String *): Unit = {
    //Execute queries
    val results = executeQueries(queries:_*)

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

  private def executeQueries(queries: String*): mutable.ArrayBuffer[List[SortedMap[String, Object]]] = {
    val results = mutable.ArrayBuffer[List[SortedMap[String, Object]]]()
    for (storage <- this.storages) {
      //HACK: Spark's storage is set using reflection as a SparkSession is not available
      H2.initialize(this.h2Engine, storage.asInstanceOf[H2Storage])
      this.sparkStorageField.set(this.sparkEngine, storage.asInstanceOf[SparkStorage])
      for (query <- queries) {
        results.append(executeQuery(query, this.h2Port))
        results.append(executeQuery(query, this.sparkPort))
      }
    }
    results
  }

  private def isEqualEnough(v1: Object, v2: Object): Unit = {
    //The results have the accuracy of floats but the JSON values are parsed to doubles
    if (v1.isInstanceOf[Double] && v2.isInstanceOf[Double]) {
      assert(v1.asInstanceOf[Double].floatValue === v2.asInstanceOf[Double].floatValue)
    } else if (v1.isInstanceOf[Float] && v2.isInstanceOf[Double]) {
      assert(v1.asInstanceOf[Float] === v2.asInstanceOf[Double].floatValue)
    } else if (v1.isInstanceOf[Double] && v2.isInstanceOf[Float]) {
      assert(v1.asInstanceOf[Double].floatValue === v2.asInstanceOf[Float])
    } else if (v1.getClass == v2.getClass) {
      assert(v1 === v2)
    } else {
      throw new IllegalArgumentException(v1.getClass + " " + v2.getClass)
    }
  }
}
