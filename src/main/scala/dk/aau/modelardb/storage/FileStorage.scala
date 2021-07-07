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

import dk.aau.modelardb.core.{Dimensions, SegmentGroup, Storage, TimeSeriesGroup}
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
//TODO: Ensure files are not merge twice if they are kept alive by a query across multiple merges.
abstract class FileStorage(rootFolder: String) extends Storage with H2Storage with SparkStorage {
  /** Instance Variables **/
  private var batchesSinceLastMerge: Int = 0
  private val segmentFolder = this.rootFolder + "segment/"
  private val segmentFolderPath: Path = new Path(segmentFolder)
  private val fileSystem: FileSystem = new Path(this.rootFolder).getFileSystem(new Configuration())

  /** Public Methods **/
  override final def open(dimensions: Dimensions): Unit = {
    if ( ! this.fileSystem.exists(this.segmentFolderPath)) {
      this.fileSystem.mkdirs(this.segmentFolderPath)
    }
  }

  override final def open(ssb: SparkSession.Builder, dimensions: Dimensions): SparkSession = {
    this.open(dimensions)

    //TODO: Determine why this have to be set when writing an ORC file
    ssb.config("spark.sql.orc.impl", "native").getOrCreate()
  }

  override final def storeTimeSeries(timeSeriesGroups: Array[TimeSeriesGroup]): Unit = {
    val finalFilePath = new Path(this.rootFolder + "time_series" + this.getFileSuffix)
    val mergeFilePath = new Path(this.rootFolder + "time_series_new" + this.getFileSuffix)
    //An unmerged file might be leftover if the system did not terminate cleanly
    this.fileSystem.delete(mergeFilePath, false)
    this.writeTimeSeriesFile(timeSeriesGroups, mergeFilePath)
    mergeAndDeleteInputFiles(finalFilePath, finalFilePath, mergeFilePath)
  }

  override final def getTimeSeries: util.HashMap[Integer, Array[Object]] = {
    val timeSeriesFile = new Path(this.rootFolder + "time_series" + this.getFileSuffix)
    if (this.fileSystem.exists(timeSeriesFile)) {
      this.readTimeSeriesFile(timeSeriesFile)
    } else {
      new util.HashMap[Integer, Array[Object]]()
    }
  }

  override final def storeModelTypes(modelsToInsert: util.HashMap[String,Integer]): Unit = {
    val finalFilePath = new Path(this.rootFolder + "model_type" + this.getFileSuffix)
    val mergeFilePath = new Path(this.rootFolder + "model_type_new" + this.getFileSuffix)
    //An unmerged file might be leftover if the system did not terminate cleanly
    this.fileSystem.delete(mergeFilePath, false)
    this.writeModelTypeFile(modelsToInsert, mergeFilePath)
    mergeAndDeleteInputFiles(finalFilePath, finalFilePath, mergeFilePath)
  }

  override final def getModelTypes: util.HashMap[String, Integer] = {
    val modelTypeFile = new Path(this.rootFolder + "model_type" + this.getFileSuffix)
    if (this.fileSystem.exists(modelTypeFile)) {
      this.readModelTypeFile(modelTypeFile)
    } else {
      new util.HashMap[String, Integer]()
    }
  }

  //H2Storage
  override final def storeSegmentGroups(segmentGroups: Array[SegmentGroup], size: Int): Unit = {
    writeSegmentGroupFile(segmentGroups, size, new Path(this.getSegmentGroupPath))
    if (shouldMerge()) {
      //TODO: register files to be merged before starting the merge so rollback is possible
      val inputFiles = listFiles(this.segmentFolderPath)
      mergeFiles(new Path(this.getSegmentGroupPath), inputFiles)
      inputFiles.forEach(ifp => this.fileSystem.delete(ifp, false))
    }
  }

  override final def getSegmentGroups(filter: TableFilter): Iterator[SegmentGroup] = {
    //TODO: How to mark files as in use until a query has finished using them? Pass a destructor function? Weak references? deleteOnExit?
    readSegmentGroupsFiles(filter, listFiles(this.segmentFolderPath))
  }

  //SparkStorage
  override final def storeSegmentGroups(sparkSession: SparkSession, df: DataFrame): Unit = {
    if ( ! shouldMerge) {
      writeSegmentGroupsFolder(sparkSession, df, this.getSegmentGroupPath)
    } else {
      val inputFolders = listFilesAndFolders(this.segmentFolderPath)
      val mergedDF = df.union(readSegmentGroupsFolders(sparkSession, Array(), inputFolders))
      writeSegmentGroupsFolder(sparkSession, mergedDF, this.getSegmentGroupPath)
      inputFolders.forEach(ifp => this.fileSystem.delete(new Path(ifp), true))
    }
  }

  override final def getSegmentGroups(sparkSession: SparkSession, filters: Array[Filter]): DataFrame = {
    //TODO: Determine why spark sometimes require that a schema be provided, is it corrupted files?
    readSegmentGroupsFolders(sparkSession, filters, listFilesAndFolders(this.segmentFolderPath))
  }

  override final def getMaxTid: Int = {
    getMaxID("tid")
  }

  override final def getMaxGid: Int = {
    getMaxID("gid")
  }

  override final def close(): Unit = {
    //Purposely empty as file descriptors are closed after use
  }

  /** Protected Methods **/
  protected def getFileSuffix: String
  protected def getMaxID(columnName: String): Int
  protected def writeTimeSeriesFile(timeSeriesGroups: Array[TimeSeriesGroup], timeSeriesFilePath: Path): Unit
  protected def readTimeSeriesFile(timeSeriesFilePath: Path): util.HashMap[Integer, Array[Object]]
  protected def writeModelTypeFile(modelsToInsert: util.HashMap[String,Integer], modelTypeFilePath: Path): Unit
  protected def readModelTypeFile(modelTypeFilePath: Path): util.HashMap[String, Integer]
  protected def writeSegmentGroupFile(segmentGroups: Array[SegmentGroup], size: Int, segmentGroupFilePath: Path): Unit
  protected def readSegmentGroupsFiles(filter: TableFilter, segmentGroupFiles: util.ArrayList[Path]): Iterator[SegmentGroup]
  protected def mergeFiles(outputFilePath: Path, inputFilesPaths: util.ArrayList[Path]): Unit
  protected def writeSegmentGroupsFolder(sparkSession: SparkSession, df: DataFrame, segmentGroupFilePath: String): Unit
  protected def readSegmentGroupsFolders(sparkSession: SparkSession, filters: Array[Filter], segmentFolders: util.ArrayList[String]): DataFrame

  /** Private Methods **/
  private def mergeAndDeleteInputFiles(outputFilePath: Path, inputFilesPaths: Path*): Unit = {
    //TODO: Backup original output before merging to protect against abnormal termination
    //Check the input files exists
    val inputFilePathsThatExists = new util.ArrayList[Path]()
    inputFilesPaths.foreach(inputFilePath => {
      if (this.fileSystem.exists(inputFilePath)) {
        inputFilePathsThatExists.add(inputFilePath)
      }
    })

    //Merge the input files
    val outputFileMerge = new Path(outputFilePath + "_merge")
    mergeFiles(outputFileMerge, inputFilePathsThatExists)

    //Delete the input files
    val inputPathsScala = inputFilePathsThatExists.asScala
    for (inputPath <- inputPathsScala) {
      this.fileSystem.delete(inputPath, false)
    }
    this.fileSystem.rename(outputFileMerge, outputFilePath)
  }

  private def shouldMerge(): Boolean = {
    //TODO: Test merging through unit tests and one HDFS
    //TODO: How often should a merged be performed? And should it be async in a different thread?
    this.batchesSinceLastMerge += 1
    if (this.batchesSinceLastMerge == 10) { //TODO: What is the correct size, or should it be time?
      this.batchesSinceLastMerge = 0
      true
    } else {
      false
    }
  }

  private def getSegmentGroupPath: String = {
    this.segmentFolder + System.currentTimeMillis() + '_' + UUID.randomUUID().toString + this.getFileSuffix
  }

  private def listFiles(folder: Path): util.ArrayList[Path] = {
    val files = this.fileSystem.listFiles(folder, false)
    val fileLists = new util.ArrayList[Path]()
    while (files.hasNext) {
      fileLists.add(files.next().getPath)
    }
    fileLists
  }

  private def listFilesAndFolders(folder: Path): util.ArrayList[String] = {
    val filesAndFolders = this.fileSystem.listStatusIterator(folder)
    val fileAndFolderList = new util.ArrayList[String]()
    while (filesAndFolders.hasNext) {
      val fileOrFolder = filesAndFolders.next().getPath.toString
      fileAndFolderList.add(fileOrFolder)
    }
    fileAndFolderList
  }
}
