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

import dk.aau.modelardb.core.{Dimensions, SegmentGroup, Storage}
import dk.aau.modelardb.engines.h2.H2Storage
import dk.aau.modelardb.engines.spark.SparkStorage
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.h2.table.TableFilter

import java.util
import java.util.UUID
import scala.collection.JavaConverters._

//TODO: Evaluate the best compression and encoding methods for the segments.
//TODO: Determine if ORC and Parquet files are read and written in the simplest way.
//TODO: determine if long or timestamp is more efficient for Apache Parquet and Apache ORC.
//TODO: Add a get dimension schema of currently stored data in storage so dimensions are not needed or can be checked.
//TODO: Ensure that FileStorage can never lose data if sub-type expose read and write methods for each table:
//      - Add mergelog listing files that have been merged but not deleted yet because a query is using it.
//      - Store list of currently active files that new queries can use and list of files to delete when not used.
abstract class FileStorage(rootFolder: String) extends Storage with H2Storage with SparkStorage {
  /** Instance Variables **/
  private var batchesSinceLastMerge: Int = 0
  private val segmentFolder: String = rootFolder + "/segment"
  private val segmentFolderPath: Path = new Path(segmentFolder)
  private val fileSystem: FileSystem = new Path(rootFolder).getFileSystem(new Configuration())

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

  //H2Storage
  override def storeSegmentGroups(segmentGroups: Array[SegmentGroup], size: Int): Unit = {
    writeSegmentGroupFiles(segmentGroups, size, this.segmentFolderPath)
    if (shouldMerge()) { //TODO: can merging be done as part of the file writing process?
      mergeSegmentGroupFiles(new Path(this.segmentFolder + "/segment"), listFiles(this.segmentFolderPath))
    }
  }

  override def getSegmentGroups(filter: TableFilter): Iterator[SegmentGroup] = {
    //TODO: How to mark files as in use until a query has finished using them? Pass a destructor function?
    readSegmentGroupsFiles(filter, this.segmentFolderPath)
  }

  //SparkStorage
  override def storeSegmentGroups(sparkSession: SparkSession, df: DataFrame): Unit = {
    if ( ! shouldMerge) {
      //Add new files for this batch to the existing folder
      writeSegmentGroupsFiles(sparkSession, df, this.segmentFolder)
    } else {
      //Writes new files with the segment groups on disk and from this batch
      val mergeSegmentsFolder = this.rootFolder + "/segment_merge"
      val mergeDF = df.union(readSegmentGroupsFiles(sparkSession, Array(), this.segmentFolder))
      writeSegmentGroupsFiles(sparkSession, mergeDF, mergeSegmentsFolder)

      //Overwrite the old segment files with the new segment file
      this.fileSystem.delete(this.segmentFolderPath, true)
      this.fileSystem.rename(new Path(mergeSegmentsFolder), this.segmentFolderPath)
    }
  }

  override def getSegmentGroups(sparkSession: SparkSession, filters: Array[Filter]): DataFrame = {
    readSegmentGroupsFiles(sparkSession, filters, this.segmentFolder)
  }

  //TODO: Are the get methods still required?
  override def getMaxTid: Int = {
    getMaxID("tid")
  }

  override def getMaxGid: Int = {
    getMaxID("gid")
  }

  override def close(): Unit = {
    //Purposely empty as file descriptors are closed after use
  }

  /** Protected Methods **/
  protected def getMaxID(columnName: String): Int //TODO: Write files that FileStorage know how to index instead of just files in a folder
  protected def writeSegmentGroupFiles(segmentGroups: Array[SegmentGroup], size: Int, segmentFolder: Path): Unit
  protected def readSegmentGroupsFiles(filter: TableFilter, segmentFolder: Path): Iterator[SegmentGroup]
  protected def mergeSegmentGroupFiles(outputFilePath: Path, inputPaths: util.ArrayList[Path]): Unit
  protected def writeSegmentGroupsFiles(sparkSession: SparkSession, df: DataFrame, segmentFolder: String): Unit
  protected def readSegmentGroupsFiles(sparkSession: SparkSession, filters: Array[Filter], segmentFolder: String): DataFrame

  protected def merge(outputFileName: String, inputNames: String*): Unit = {
    val slashedRootFolder = this.rootFolder + "/"
    val inputPaths = new util.ArrayList[Path]()
    inputNames.foreach(inputFile => {
      val path = new Path(slashedRootFolder + inputFile)
      if (this.fileSystem.exists(path)) {
        inputPaths.add(path)
      }
    })
    val outputFilePath = new Path(slashedRootFolder + outputFileName)
    mergeSegmentGroupFiles(outputFilePath, inputPaths)

    //Delete the old files
    val inputPathsScala = inputPaths.asScala
    for (inputPath <- inputPathsScala) {
      this.fileSystem.delete(inputPath, false)
    }
    val outputMerge  = new Path(outputFilePath + "_merge")
    this.fileSystem.rename(outputMerge, outputFilePath)
    this.batchesSinceLastMerge = 0
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
    true //TODO: Test merging through unit tests and one HDFS
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
