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
//TODO: Maybe split segment into a hot and cold folder, so new files are stored in hot and later merged into cold.
//TODO: Ensure H2 and Spark can use FileStorage created by the other system, move Spark output to Folder+PartName?
//TODO: Ensure the minimum number of file listing and constructions of new Path objects.
abstract class FileStorage(rootFolder: String) extends Storage with H2Storage with SparkStorage {
  /** Instance Variables **/
  private var batchesSinceLastMerge: Int = 0
  private val segmentFolder = this.rootFolder + "segment/"
  private val segmentFolderPath: Path = new Path(segmentFolder)
  private val fileSystem: FileSystem = new Path(this.rootFolder).getFileSystem(new Configuration())

  /** Public Methods **/
  override def open(dimensions: Dimensions): Unit = {
    if ( ! this.fileSystem.exists(this.segmentFolderPath)) {
      this.fileSystem.mkdirs(this.segmentFolderPath)
    }
  }

  override def open(ssb: SparkSession.Builder, dimensions: Dimensions): SparkSession = {
    this.open(dimensions)

    //TODO: Determine why this have to be set when writing an ORC file
    ssb.config("spark.sql.orc.impl", "native").getOrCreate()
  }

  //H2Storage
  override def storeSegmentGroups(segmentGroups: Array[SegmentGroup], size: Int): Unit = {
    writeSegmentGroupFile(segmentGroups, size, this.getSegmentPartPath)
    if (shouldMerge()) {
      //TODO: register files to be merged before starting the merge so rollback is possible
      val inputFiles = listFiles(this.segmentFolderPath)
      mergeFiles(new Path(getSegmentPartPath), inputFiles)
      inputFiles.forEach(ifp => this.fileSystem.delete(ifp, false))
    }
  }

  override def getSegmentGroups(filter: TableFilter): Iterator[SegmentGroup] = {
    //TODO: How to mark files as in use until a query has finished using them? Pass a destructor function? Weak references? deleteOnExit?
    readSegmentGroupsFiles(filter, listFiles(this.segmentFolderPath))
  }

  //SparkStorage
  override def storeSegmentGroups(sparkSession: SparkSession, df: DataFrame): Unit = {
    if ( ! shouldMerge) {
      //Add new files for this batch to the existing folder
      writeSegmentGroupsFile(sparkSession, df, this.segmentFolder)
    } else {
      //Create a new segment folder with the segment groups on disk and from this batch
      val mergeSegmentsFolder = this.rootFolder + "segment_merge"
      val mergeDF = df.union(readSegmentGroupsFiles(sparkSession, Array(),
        this.listFiles(new Path(this.segmentFolder))))
      writeSegmentGroupsFile(sparkSession, mergeDF, mergeSegmentsFolder)

      //Overwrite the old segment folder with the new segment folder
      this.fileSystem.delete(this.segmentFolderPath, true)
      this.fileSystem.rename(new Path(mergeSegmentsFolder), this.segmentFolderPath)
    }
  }

  override def getSegmentGroups(sparkSession: SparkSession, filters: Array[Filter]): DataFrame = {
    readSegmentGroupsFiles(sparkSession, filters, listFiles(this.segmentFolderPath))
  }

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
  protected def getMaxID(columnName: String): Int
  protected def writeSegmentGroupFile(segmentGroups: Array[SegmentGroup], size: Int, segmentGroupFile: String): Unit
  protected def readSegmentGroupsFiles(filter: TableFilter, segmentGroupFiles: util.ArrayList[Path]): Iterator[SegmentGroup]
  protected def mergeFiles(outputFileRelativePath: Path, inputFilesRelativePaths: util.ArrayList[Path]): Unit
  protected def writeSegmentGroupsFile(sparkSession: SparkSession, df: DataFrame, segmentGroupFile: String): Unit
  protected def readSegmentGroupsFiles(sparkSession: SparkSession, filters: Array[Filter], segmentFolders: util.ArrayList[Path]): DataFrame

  protected def mergeAndDeleteInputFiles(outputFileRelativePath: String, inputFilesRelativePaths: String*): Unit = {
    val inputPaths = new util.ArrayList[Path]()
    inputFilesRelativePaths.foreach(inputFileRelativePath => {
      val path = new Path(this.rootFolder + inputFileRelativePath)
      println(path)
      if (this.fileSystem.exists(path)) {
        inputPaths.add(path)
      }
    })
    val outputFilePath = new Path(this.rootFolder + outputFileRelativePath)
    val outputFileMerge = new Path(outputFilePath + "_merge")
    mergeFiles(outputFileMerge, inputPaths)

    //Delete the old files
    val inputPathsScala = inputPaths.asScala
    for (inputPath <- inputPathsScala) {
      this.fileSystem.delete(inputPath, false)
    }
    this.fileSystem.rename(outputFileMerge, outputFilePath)
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

  private def getSegmentPartPath: String = {
    //The filenames purposely use the same structure as used by Apache Spark to make listing the files simpler
    this.segmentFolder + "part-" +  UUID.randomUUID().toString + "-" + System.currentTimeMillis()
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
