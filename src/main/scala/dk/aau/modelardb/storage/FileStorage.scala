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
import dk.aau.modelardb.core.{Dimensions, SegmentGroup, TimeSeriesGroup}
import dk.aau.modelardb.engines.h2.H2Storage
import dk.aau.modelardb.engines.spark.SparkStorage
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.h2.table.TableFilter

import java.util.UUID
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.mutable
import java.lang.ref.{PhantomReference, ReferenceQueue}

//TODO: Ensure that FileStorage can never lose data if sub-type expose read and write methods for each table:
//      - Add mergelog listing files that have been merged but not deleted yet because a query is using it.
//      - Store list of currently active files that new queries can use and list of files to delete when not used.
//TODO: Ensure H2 and Spark can use FileStorage created by the other system, move Spark output to Folder+PartName?
//TODO: Ensure files are not merge twice if they are kept alive by a query across multiple merges.
//TODO: Test everything thoroughly on both local filesystem and HDFS, and using unittest
abstract class FileStorage(rootFolder: String) extends Storage with H2Storage with SparkStorage {
  //Warn users that FileStorage storage layers should be considered experimental
  Static.warn("ModelarDB: using experimental storage layer " + this.getClass)

  /** Instance Variables **/
  private val segmentFolder = this.rootFolder + "segment/"
  private val segmentFolderPath: Path = new Path(segmentFolder)
  private val fileSystem: FileSystem = new Path(this.rootFolder).getFileSystem(new Configuration())

  //Variables for tracking when to merge and which files can be included in a merge
  private val batchesBetweenMerges: Int = 500
  private var batchesSinceLastMerge: Int = 0
  private val segmentGroupFilesInQuery = mutable.HashMap[Object, Integer]().withDefaultValue(0)
  private val segmentGroupFilesIterators = mutable.HashMap[Object, mutable.ArrayBuffer[Object]]()
  private val phantomReferenceQueue = new ReferenceQueue[Object]()
  private val fileStorageLock = new ReentrantReadWriteLock()

  /** Public Methods **/
  override final def open(dimensions: Dimensions): Unit = {
    if ( ! this.fileSystem.exists(this.segmentFolderPath)) {
      this.fileSystem.mkdirs(this.segmentFolderPath)
    } else {
      this.recover() //Nothing to recover if the system was terminated before the segment folder was created
      val files = this.fileSystem.listFiles(segmentFolderPath, false)
      while (files.hasNext) {
        this.batchesSinceLastMerge += 1
        files.next()
      }
      this.batchesSinceLastMerge -= 1 //The main file is not a batch
    }
  }

  override final def storeTimeSeries(timeSeriesGroups: Array[TimeSeriesGroup]): Unit = {
    val outputFilePath = new Path(this.rootFolder + "time_series" + this.getFileSuffix)
    val newFilePath = new Path(this.rootFolder + "time_series" + this.getFileSuffix + "_new")
    this.writeTimeSeriesFile(timeSeriesGroups, newFilePath)
    this.mergeAndDeleteInputFiles(outputFilePath, outputFilePath, newFilePath)
  }

  override final def getTimeSeries: mutable.HashMap[Integer, Array[Object]] = {
    val timeSeriesFile = new Path(this.rootFolder + "time_series" + this.getFileSuffix)
    if (this.fileSystem.exists(timeSeriesFile)) {
      this.readTimeSeriesFile(timeSeriesFile)
    } else {
      mutable.HashMap[Integer, Array[Object]]()
    }
  }

  override final def storeModelTypes(modelsToInsert: mutable.HashMap[String,Integer]): Unit = {
    val outputFilePath = new Path(this.rootFolder + "model_type" + this.getFileSuffix)
    val newFilePath = new Path(this.rootFolder + "model_type" + this.getFileSuffix  + "_new")
    this.writeModelTypeFile(modelsToInsert, newFilePath)
    this.mergeAndDeleteInputFiles(outputFilePath, outputFilePath, newFilePath)
  }

  override final def getModelTypes: mutable.HashMap[String, Integer] = {
    val modelTypeFile = new Path(this.rootFolder + "model_type" + this.getFileSuffix)
    if (this.fileSystem.exists(modelTypeFile)) {
      this.readModelTypeFile(modelTypeFile)
    } else {
      mutable.HashMap[String, Integer]()
    }
  }

  //H2Storage
  override final def storeSegmentGroups(segmentGroups: Array[SegmentGroup], size: Int): Unit = {
    writeSegmentGroupFile(segmentGroups, size, new Path(this.getSegmentGroupPath))
    if (shouldMerge()) {
      //TODO: register files to be merged before starting the merge so rollback is possible, and put get files to merge into method
      this.unlockSegmentGroupFilesFolders()
      val allFiles = this.listFiles(this.segmentFolderPath)
      val filesToMerge = allFiles.filter(ff => ! this.segmentGroupFilesInQuery.contains(ff))
      this.mergeFiles(new Path(this.getSegmentGroupPath), filesToMerge)
      filesToMerge.foreach(ifp => this.fileSystem.delete(ifp, false))
    }
  }

  override final def getSegmentGroups(filter: TableFilter): Iterator[SegmentGroup] = {
    val segmentGroupFiles = listFiles(this.segmentFolderPath)
    val iterator = this.readSegmentGroupsFiles(filter, segmentGroupFiles)
    this.lockSegmentGroupFilesAndFolders(segmentGroupFiles.asInstanceOf[mutable.ArrayBuffer[Object]], iterator)
    iterator
  }

  //SparkStorage
  override final def open(ssb: SparkSession.Builder, dimensions: Dimensions): SparkSession = {
    this.open(dimensions)

    //TODO: Determine why this have to be set when writing an ORC file
    ssb.config("spark.sql.orc.impl", "native").getOrCreate()
  }

  override final def storeSegmentGroups(sparkSession: SparkSession, df: DataFrame): Unit = {
    if ( ! shouldMerge) {
      writeSegmentGroupsFolder(sparkSession, df, this.getSegmentGroupPath)
    } else {
      //TODO: register files to be merged before starting the merge so rollback is possible, and put get files to merge into method
      this.unlockSegmentGroupFilesFolders()
      val allFilesAndFolders = this.listFilesAndFolders(this.segmentFolderPath)
      val filesAndFoldersToMerge = allFilesAndFolders.filter(ff => ! this.segmentGroupFilesInQuery.contains(ff))
      val mergedDF = df.union(readSegmentGroupsFolders(sparkSession, Array(), filesAndFoldersToMerge))
      this.writeSegmentGroupsFolder(sparkSession, mergedDF, this.getSegmentGroupPath)
      filesAndFoldersToMerge.foreach(ifp => this.fileSystem.delete(new Path(ifp), true))
    }
  }

  override final def getSegmentGroups(sparkSession: SparkSession, filters: Array[Filter]): DataFrame = {
    //TODO: Determine why Spark sometimes require that a schema be provided, is it corrupted files?
    val segmentGroupFilesAndFolders = this.listFilesAndFolders(this.segmentFolderPath)
    val df = readSegmentGroupsFolders(sparkSession, filters, segmentGroupFilesAndFolders)
    this.lockSegmentGroupFilesAndFolders(segmentGroupFilesAndFolders.asInstanceOf[mutable.ArrayBuffer[Object]], df)
    df
  }

  override final def getMaxTid: Int = {
    getMaxID("tid", new Path(this.rootFolder + "time_series" + this.getFileSuffix))
  }

  override final def getMaxGid: Int = {
    getMaxID("gid", new Path(this.rootFolder + "time_series" + this.getFileSuffix))
  }

  override final def close(): Unit = {
    //Purposely empty as file descriptors are closed after use
  }

  /** Protected Methods **/
  protected def getFileSuffix: String
  protected def getMaxID(columnName: String, timeSeriesFilePath: Path): Int
  protected def mergeFiles(outputFilePath: Path, inputFilesPaths: mutable.ArrayBuffer[Path]): Unit
  protected def writeTimeSeriesFile(timeSeriesGroups: Array[TimeSeriesGroup], timeSeriesFilePath: Path): Unit
  protected def readTimeSeriesFile(timeSeriesFilePath: Path): mutable.HashMap[Integer, Array[Object]]
  protected def writeModelTypeFile(modelsToInsert: mutable.HashMap[String,Integer], modelTypeFilePath: Path): Unit
  protected def readModelTypeFile(modelTypeFilePath: Path): mutable.HashMap[String, Integer]
  protected def writeSegmentGroupFile(segmentGroups: Array[SegmentGroup], size: Int, segmentGroupFilePath: Path): Unit
  protected def readSegmentGroupsFiles(filter: TableFilter, segmentGroupFiles: mutable.ArrayBuffer[Path]): Iterator[SegmentGroup]
  protected def writeSegmentGroupsFolder(sparkSession: SparkSession, df: DataFrame, segmentGroupFilePath: String): Unit
  protected def readSegmentGroupsFolders(sparkSession: SparkSession, filters: Array[Filter], segmentFolders: mutable.ArrayBuffer[String]): DataFrame

  protected final def lockSegmentGroupFilesAndFolders(segmentGroupFiles: mutable.ArrayBuffer[Object], iterator: Object): Unit = {
    this.fileStorageLock.writeLock().lock()
    segmentGroupFiles.foreach(sgf => this.segmentGroupFilesInQuery(sgf) += 1)
    val phantomReferenceToIterator = new PhantomReference(iterator, this.phantomReferenceQueue)
    this.segmentGroupFilesIterators.put(phantomReferenceToIterator, segmentGroupFiles)
    this.fileStorageLock.writeLock().unlock()
  }

  protected final def unlockSegmentGroupFilesFolders(): Unit = {
    this.fileStorageLock.writeLock().lock()
    var phantomReferenceToIterator = this.phantomReferenceQueue.poll
    while (phantomReferenceToIterator != null) {
      val segmentGroupFiles = this.segmentGroupFilesIterators(phantomReferenceToIterator)
      segmentGroupFiles.foreach(sgf => {
        this.segmentGroupFilesInQuery(sgf) -= 1
        if (this.segmentGroupFilesInQuery(sgf) == 0) {
          this.segmentGroupFilesInQuery.remove(sgf)
        }
      })
      phantomReferenceToIterator.clear()
      phantomReferenceToIterator = this.phantomReferenceQueue.poll
    }
    this.fileStorageLock.writeLock().unlock()
  }

  /** Private Methods **/
  private def recover(): Unit = {
    //Deletes files leftover if the system terminates abnormally before ingestion begins
    this.deleteNewMergeAndBackup("time_series")
    this.deleteNewMergeAndBackup("model_type")

    //Recover the original files if the system terminates abnormally while merging segment files
    //TODO: if merge done use merged files and delete old, if not done delete metadata and use fold files
  }

  private def deleteNewMergeAndBackup(fileNameWithoutSuffix: String): Unit = {
    this.fileSystem.delete( //Terminated before merging the old and new time series
      new Path(this.rootFolder + fileNameWithoutSuffix + this.getFileSuffix + "_new"), true)
    this.fileSystem.delete( //Terminated before renaming the merged file
      new Path(this.rootFolder + fileNameWithoutSuffix + this.getFileSuffix + "_merge"), true)
    this.fileSystem.delete( //Terminated before deleting the backup file
      new Path(this.rootFolder + fileNameWithoutSuffix + this.getFileSuffix + "_backup"), true)
  }

  private def mergeAndDeleteInputFiles(outputFilePath: Path, inputFilesPaths: Path*): Unit = {
    //Check the input files exists and merge them
    val inputFilePathsThatExists = mutable.ArrayBuffer[Path]()
    inputFilesPaths.foreach(inputFilePath => {
      if (this.fileSystem.exists(inputFilePath)) {
        inputFilePathsThatExists += inputFilePath
      }
    })
    val outputFilePathMerge = new Path(outputFilePath + "_merge")
    mergeFiles(outputFilePathMerge, inputFilePathsThatExists)

    //Backup the original output file if it exists, and write the new file
    if (this.fileSystem.exists(outputFilePath)) {
      val outputFilePathBackup = new Path(outputFilePath + "_backup")
      this.fileSystem.rename(outputFilePath, outputFilePathBackup)
      this.fileSystem.rename(outputFilePathMerge, outputFilePath)
      this.fileSystem.delete(outputFilePathBackup, true)
      inputFilePathsThatExists -= outputFilePath //Do not delete merged file
    } else {
      this.fileSystem.rename(outputFilePathMerge, outputFilePath)
    }

    //Delete the input files
    inputFilePathsThatExists.foreach(inputFilePath => this.fileSystem.delete(inputFilePath, false))
  }

  private def shouldMerge(): Boolean = {
    this.batchesSinceLastMerge += 1
    if (this.batchesSinceLastMerge == this.batchesBetweenMerges) {
      this.batchesSinceLastMerge = 0
      true
    } else {
      false
    }
  }

  private def getSegmentGroupPath: String = {
    this.segmentFolder + System.currentTimeMillis() + '_' + UUID.randomUUID().toString + this.getFileSuffix
  }

  private def listFiles(folder: Path): mutable.ArrayBuffer[Path] = {
    val files = this.fileSystem.listFiles(folder, false)
    val fileLists = mutable.ArrayBuffer[Path]()
    while (files.hasNext) {
      fileLists += files.next().getPath
    }
    fileLists
  }

  private def listFilesAndFolders(folder: Path): mutable.ArrayBuffer[String] = {
    val filesAndFolders = this.fileSystem.listStatusIterator(folder)
    val fileAndFolderList = mutable.ArrayBuffer[String]()
    while (filesAndFolders.hasNext) {
      val fileOrFolder = filesAndFolders.next().getPath.toString
      fileAndFolderList += fileOrFolder
    }
    fileAndFolderList
  }
}
