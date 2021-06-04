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
package dk.aau.modelardb.storage

import dk.aau.modelardb.core.utility.Static
import dk.aau.modelardb.core.{Dimensions, Storage}
import dk.aau.modelardb.engines.h2.H2Storage
import dk.aau.modelardb.engines.spark.SparkStorage
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession, sources}
import org.apache.spark.sql.sources.Filter

import java.sql.Timestamp
import java.util
import java.util.UUID

//TODO: Ensure that FileStorage can never lose data if sub-type expose read and write methods for each table:
//      - Add mergelog listing files that have been merged but not deleted yet because a query is using it.
//      - Store list of currently active files that new queries can use and list of files to delete when not used.
abstract class FileStorage(rootFolder: String) extends Storage with H2Storage with SparkStorage {
  /** Instance Variables **/
  protected var batchesSinceLastMerge: Int = 0
  protected val segmentFolder: String = rootFolder + "/segment"
  protected val segmentFolderPath: Path = new Path(segmentFolder)
  protected val fileSystem: FileSystem = new Path(rootFolder).getFileSystem(new Configuration())

  /** Public Methods **/
  override def open(dimensions: Dimensions): Unit = {
    //Purposely empty as file descriptors are created as necessary
  }

  override def open(ssb: SparkSession.Builder, dimensions: Dimensions): SparkSession = {
    //The segment folder must exist as it is joined with each new batch
    val segment = new Path(this.rootFolder + "/segment")
    val fs = segment.getFileSystem(new Configuration())
    if ( ! fs.exists(segment)) {
      fs.mkdirs(segment)
    }

    //TODO: Figure out why this have to be changed when writing an ORC file
    ssb.config("spark.sql.orc.impl", "native").getOrCreate()
  }


  override def getMaxTid: Int = {
    getMaxID(0)
  }

  override def getMaxGid: Int = {
    getMaxID(3)
  }

  override def close(): Unit = {
    //Purposely empty as file descriptors are close after use
  }

  /** Protected Methods **/
  protected def getMaxID(column: Int): Int

  protected def merge(inputPaths: util.ArrayList[Path], outputFilePath: String): Unit

  protected def merge(folder: String, prefix: String, outputFileName: String): Unit = {
    val files = listFiles(new Path(folder))
    files.removeIf((path: Path) => ! path.getName.startsWith(prefix))
    merge(files, folder + "/" + outputFileName)
  }

  protected def shouldMerge(): Boolean = {
    //TODO: How often should a merged be performed? And should it be async in a different thread?
    this.batchesSinceLastMerge += 1
    if (this.batchesSinceLastMerge == 500) {
      this.batchesSinceLastMerge = 0
      true
    } else {
      false
    }
    true
  }

  protected def getSegmentPartPath(suffix: String): String = {
    //The filenames purposely use the same structure as used by Apache Spark to make listing them simpler
    this.segmentFolder + "/part-" +  UUID.randomUUID().toString + "-" + System.currentTimeMillis() + suffix
  }

  protected def listFiles(folder: Path): util.ArrayList[Path] = {
    println(folder)
    val files = this.fileSystem.listFiles(folder, false)
    val fileLists = new util.ArrayList[Path]()
    while (files.hasNext) {
      fileLists.add(files.next().getPath)
    }
    fileLists
  }
  
  protected def pushDownSparkFilters(df: DataFrame, filters: Array[Filter]): DataFrame = {
    var withPredicates = df
    for (filter: Filter <- filters) {
      filter match {
        //Predicate push-down for gid using SELECT * FROM segment with GID = ? and gid IN (..)
        case sources.GreaterThan("gid", value: Int) => withPredicates = withPredicates.filter(s"sid > $value")
        case sources.GreaterThanOrEqual("gid", value: Int) => withPredicates = withPredicates.filter(s"sid >= $value")
        case sources.LessThan("gid", value: Int) => withPredicates = withPredicates.filter(s"sid < $value")
        case sources.LessThanOrEqual("gid", value: Int) => withPredicates = withPredicates.filter(s"sid <= $value")
        case sources.EqualTo("gid", value: Int) => withPredicates = withPredicates.filter(s"sid = $value")
        case sources.In("gid", value: Array[Any]) => withPredicates = withPredicates.filter(value.mkString("sid IN (", ",", ")"))

        //Predicate push-down for et using SELECT * FROM segment WHERE start_time <=> ?
        case sources.GreaterThan("start_time", value: Timestamp) => withPredicates.filter(s"start_time > '$value'")
        case sources.GreaterThanOrEqual("start_time", value: Timestamp) => withPredicates.filter(s"start_time >= '$value'")
        case sources.LessThan("start_time", value: Timestamp) => withPredicates.filter(s"start_time < '$value'")
        case sources.LessThanOrEqual("start_time", value: Timestamp) => withPredicates.filter(s"start_time <= '$value'")
        case sources.EqualTo("start_time", value: Timestamp) => withPredicates.filter(s"start_time = '$value'")

        //Predicate push-down for et using SELECT * FROM segment WHERE end_time <=> ?
        case sources.GreaterThan("end_time", value: Timestamp) => withPredicates.filter(s"end_time > '$value'")
        case sources.GreaterThanOrEqual("end_time", value: Timestamp) => withPredicates.filter(s"end_time >= '$value'")
        case sources.LessThan("end_time", value: Timestamp) => withPredicates.filter(s"end_time < '$value'")
        case sources.LessThanOrEqual("end_time", value: Timestamp) => withPredicates.filter(s"end_time <= '$value'")
        case sources.EqualTo("end_time", value: Timestamp) => withPredicates.filter(s"end_time = '$value'")

        //The predicate cannot be supported by the segment view so all we can do is inform the user
        case p => Static.warn("ModelarDB: unsupported predicate for FileStorage " + p, 120); null
      }
    }
    withPredicates
  }
}
