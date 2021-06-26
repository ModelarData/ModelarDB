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

import dk.aau.modelardb.core.{Dimensions, Storage}
import dk.aau.modelardb.engines.h2.H2Storage
import dk.aau.modelardb.engines.spark.SparkStorage
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.util
import java.util.UUID

//TODO: Evaluate the best compression and encoding methods for the segments.
//TODO: Determine if ORC and Parquet files are read and written in the simplest way.
//TODO: determine if long or timestamp is more efficient for Apache Parquet and Apache ORC.
//TODO: Add a get dimension schema of currently stored data in storage so dimensions are not needed or can be checked.
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

    //TODO: Figure out why this have to be set when writing an ORC file
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

  protected def merge(outputFilePath: Path, inputPaths: util.ArrayList[Path]): Unit

  protected def merge(outputFileName: String, inputNames: String*): Unit = {
    val slashedRootFolder = this.rootFolder + "/"
    val inputPaths = new util.ArrayList[Path]()
    inputNames.foreach(inputFile => {
      val path = new Path(slashedRootFolder + inputFile)
      if (this.fileSystem.exists(path)) {
        inputPaths.add(path)
      }
    })
    merge(new Path(slashedRootFolder + outputFileName), inputPaths)
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
    true //TODO: Test merging through unit tests
  }

  protected def getSegmentPartPath(suffix: String): String = {
    //The filenames purposely use the same structure as used by Apache Spark to make listing them simpler
    this.segmentFolder + "/part-" +  UUID.randomUUID().toString + "-" + System.currentTimeMillis() + suffix
  }

  protected def listFiles(folder: Path): util.ArrayList[Path] = {
    val files = this.fileSystem.listFiles(folder, false)
    val fileLists = new util.ArrayList[Path]()
    while (files.hasNext) {
      fileLists.add(files.next().getPath)
    }
    fileLists
  }
}