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
//TODO: share implementation with ParquetStorage through a FileStorage abstract class?
import dk.aau.modelardb.core.utility.{Pair, ValueFunction}
import dk.aau.modelardb.engines.h2.H2Storage

import java.io.FileNotFoundException
import java.sql.Timestamp
import java.util
import java.util.UUID
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.h2.table.TableFilter

import scala.collection.mutable
//TODO: Why RLE integer errors when reading multiple batches with Spark but not with orc-core?
import org.apache.hadoop.hive.ql.exec.vector._
import org.apache.orc.{CompressionKind, OrcFile, Reader, TypeDescription, Writer}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.JavaConverters._
import dk.aau.modelardb.core.{Dimensions, SegmentGroup, Storage, TimeSeriesGroup}
import dk.aau.modelardb.engines.spark.SparkStorage

class ORCStorage(location: String) extends Storage with H2Storage with SparkStorage {

  /** Public Methods **/
  override def open(dimensions: Dimensions): Unit = {
    //The ORC files are automatically created when data is written
  }

  override def initialize(timeSeriesGroups: Array[TimeSeriesGroup],
                          derivedTimeSeries: util.HashMap[Integer, Array[Pair[String, ValueFunction]]],
                          dimensions: Dimensions, modelNames: Array[String]): Unit = {
    //Inserts the metadata for the sources defined in the configuration file (Sid, Scaling, Resolution, Gid, Dimensions)
    var schema = TypeDescription.createStruct()
      .addField("tid", TypeDescription.createInt())
      .addField("scaling_factor", TypeDescription.createFloat())
      .addField("sampling_interval", TypeDescription.createInt())
      .addField("gid", TypeDescription.createInt())
    for (dim <- dimensions.getColumns) {
      //HACK: Assumes all dimensions are strings
      schema.addField(dim, TypeDescription.createString())
    }
    val sourceOut = getWriter(this.location + "/time_series_new.orc", schema)
    var batch = sourceOut.getSchema.createRowBatch()

    for (tsg <- timeSeriesGroups) {
      for (ts <- tsg.getTimeSeries) {
        val row = { batch.size += 1; batch.size - 1 } //batch.size++
        batch.cols(0).asInstanceOf[LongColumnVector].vector(row) = ts.tid
        batch.cols(1).asInstanceOf[DoubleColumnVector].vector(row) = ts.scalingFactor
        batch.cols(2).asInstanceOf[LongColumnVector].vector(row) = ts.samplingInterval
        batch.cols(3).asInstanceOf[LongColumnVector].vector(row) = tsg.gid
        for (mi <- dimensions.get(ts.source).zipWithIndex) {
          //HACK: Assumes all dimensions are strings
          batch.cols(4 + mi._2).asInstanceOf[BytesColumnVector].setVal(row, mi._1.toString.getBytes)
        }

        if (batch.size == batch.getMaxSize) {
          flush(sourceOut, batch)
        }
      }
    }
    flush(sourceOut, batch)
    sourceOut.close()
    merge(this.location, "time_series")

    //Extracts all metadata for the sources in storage
    val sourceIn = getReader(new Path(this.location + "/time_series.orc"))
    var rows = sourceIn.rows()
    batch = sourceIn.getSchema.createRowBatch()
    val columns = batch.cols.length
    val sourcesInStorage = new util.HashMap[Integer, Array[Object]]()
    //TODO: Fix java.lang.NullPointerException in readStripeFooter when using orc-core 1.6.4
    while (rows.nextBatch(batch)) {
      for (row <- 0 until batch.size) {
        //The metadata is stored as (Sid => Scaling, Resolution, Gid, Dimensions)
        val metadata = new util.ArrayList[Object]()
        val sid = batch.cols(0).asInstanceOf[LongColumnVector].vector(row)
        metadata.add(batch.cols(1).asInstanceOf[DoubleColumnVector].vector(row).toFloat.asInstanceOf[Object])
        metadata.add(batch.cols(2).asInstanceOf[LongColumnVector].vector(row).toInt.asInstanceOf[Object])
        metadata.add(batch.cols(3).asInstanceOf[LongColumnVector].vector(row).toInt.asInstanceOf[Object])
        for (column <- 4 until columns) {
          //HACK: Assumes all dimensions are strings
          metadata.add(batch.cols(column).asInstanceOf[BytesColumnVector].toString(row))
        }
        sourcesInStorage.put(sid.toInt, metadata.toArray)
      }
      rows.close()
    }

    //Extracts all model names from storage
    val modelsInStorage = new util.HashMap[String, Integer]()
    try {
      val modelIn = getReader(new Path(this.location + "/model_type.orc"))
      rows = modelIn.rows()
      batch = modelIn.getSchema.createRowBatch()
      while (rows.nextBatch(batch)) {
        for (row <- 0 until batch.size) {
          val mid = batch.cols(0).asInstanceOf[LongColumnVector].vector(row).toInt
          val cp = batch.cols(1).asInstanceOf[BytesColumnVector].toString(row)
          modelsInStorage.put(cp, mid)
        }
      }
    } catch {
      case _: FileNotFoundException => //Ignore
    }

    //Initializes the storage caches
    val modelsToInsert = super.initializeCaches(modelNames, dimensions, modelsInStorage, sourcesInStorage, derivedTimeSeries)

    //Inserts the model's names for all of the models in the configuration file but not in storage
    schema = TypeDescription.createStruct()
      .addField("mid", TypeDescription.createInt())
      .addField("name", TypeDescription.createString())
    val modelOut = getWriter(this.location + "/model_type.orc_new", schema)
    batch = modelOut.getSchema.createRowBatch()

    for ((k, v) <- modelsToInsert.asScala) {
      val row = { batch.size += 1; batch.size - 1 } //batch++
      batch.cols(0).asInstanceOf[LongColumnVector].vector(row) = v.intValue()
      batch.cols(1).asInstanceOf[BytesColumnVector].setVal(row, k.getBytes)

      if (batch.size == batch.getMaxSize) {
        flush(modelOut, batch)
      }
    }
    flush(modelOut, batch)
    modelOut.close()
    merge(this.location, "model_type")
  }

  override def getMaxTid: Int = getMaxID(0)

  override def getMaxGid: Int = getMaxID(3)

  override def close(): Unit = {
    //The ORC files are all closed after data is written
  }

  //H2Storage
  override def storeSegmentGroups(segmentGroups: Array[SegmentGroup], size: Int): Unit = {
    val segmentOut = getWriter(getSegmentPartLocation, this.segmentSchema)
    val batch = segmentOut.getSchema.createRowBatch()

    for (segmentGroup <- segmentGroups.take(size)) {
      //TODO: is long or timestamp more efficient?
      val row = { batch.size += 1; batch.size - 1 } //batch.size++
      batch.cols(0).asInstanceOf[LongColumnVector].vector(row) = segmentGroup.gid
      batch.cols(1).asInstanceOf[TimestampColumnVector].set(row, new Timestamp(segmentGroup.startTime))
      batch.cols(2).asInstanceOf[TimestampColumnVector].set(row, new Timestamp(segmentGroup.endTime))
      batch.cols(3).asInstanceOf[LongColumnVector].vector(row) = segmentGroup.mtid
      batch.cols(4).asInstanceOf[BytesColumnVector].setVal(row, segmentGroup.model)
      batch.cols(5).asInstanceOf[BytesColumnVector].setVal(row, segmentGroup.offsets)
      if (batch.size == batch.getMaxSize) {
        flush(segmentOut, batch)
      }
    }
    flush(segmentOut, batch)
    segmentOut.close()

    //After N batches the ORC files are merged together to reduce the overhead of reading many small files
    if (shouldMerge()) {
      merge(listFiles(this.segmentFolderPath), getSegmentPartLocation)
      this.batchesSinceLastMerge = 0
    }
  }

  override def getSegmentGroups(filter: TableFilter): Iterator[SegmentGroup] = {
    //Reads all non-hidden files in the folder so _SUCCESS is included, but it just treated as an empty ORC file
    val segmentFiles = listFiles(this.segmentFolderPath).iterator()
    val segmentGroups = new mutable.ArrayBuffer[SegmentGroup]

    //TODO: how can segment groups be read without returning all rows? Maybe costume iterator for ORC and RDBMS?
    while (segmentFiles.hasNext) {
      val segmentFile = getReader(segmentFiles.next())
      val rows = segmentFile.rows()
      val batch = segmentFile.getSchema.createRowBatch()

      while (rows.nextBatch(batch)) {
        for (row <- 0 until batch.size) {
          val gid = batch.cols(0).asInstanceOf[LongColumnVector].vector(row).toInt
          //val startTime = batch.cols(1).asInstanceOf[TimestampColumnVector].asScratchTimestamp(row).getTime
          val startTime = batch.cols(1).asInstanceOf[LongColumnVector].vector(row)
          //val endTime = batch.cols(2).asInstanceOf[TimestampColumnVector].asScratchTimestamp(row).getTime
          val endTime = batch.cols(2).asInstanceOf[LongColumnVector].vector(row)
          val mid = batch.cols(3).asInstanceOf[LongColumnVector].vector(row).toInt
          val parameters = readBytes(batch.cols(4).asInstanceOf[BytesColumnVector], row)
          val offsets = readBytes(batch.cols(5).asInstanceOf[BytesColumnVector], row)
          segmentGroups.append(new SegmentGroup(gid, startTime, endTime, mid, parameters, offsets))
        }
      }
      rows.close()
    }
    segmentGroups.iterator
  }

  //SparkStorage
  override def open(ssb: SparkSession.Builder, dimensions: Dimensions): SparkSession = {
    //The segment folder must exist as it is joined with each new batch
    val segment = new Path(this.location + "/segment")
    val fs = segment.getFileSystem(new Configuration())
    if ( ! fs.exists(segment)) {
      fs.mkdirs(segment)
    }

    //TODO: Figure out why this have to be changed when writing an ORC file
    this.sparkSession = ssb.config("spark.sql.orc.impl", "native").getOrCreate()
    this.sparkSession
  }

  override def storeSegmentGroups(sparkSession: SparkSession, rdd: RDD[Row]): Unit = {
    if ( ! shouldMerge) {
      //Add new ORC files for this batch to the existing folder
      this.sparkSession.createDataFrame(rdd, this.segmentSparkSchema)
        .write.mode(SaveMode.Append).orc(this.segmentFolder)
    } else {
      //Writes new ORC files with the segment on disk and from this batch
      val rddDF = this.sparkSession.createDataFrame(rdd, this.segmentSparkSchema)
        .union(this.sparkSession.read.schema(this.segmentSparkSchema).orc(this.segmentFolder))
      val newSegmentFolder = new Path(this.location + "/segment_new")
      rddDF.write.orc(newSegmentFolder.toString)

      //Overwrite the old segment folder with the new segment folder
      this.fileSystem.delete(this.segmentFolderPath, true)
      this.fileSystem.rename(newSegmentFolder, this.segmentFolderPath)
    }
  }

  override def getSegmentGroups(sparkSession: SparkSession, filters: Array[Filter]): RDD[Row] = {
    this.sparkSession.read.orc(this.location + "/segment").rdd
  }

  /** Private Methods **/
  private def getMaxID(index: Int): Int = {
    val sources = try {
      getReader(new Path(this.location + "/time_series.orc"))
    } catch {
      case _: FileNotFoundException => return 0
    }
    val rows = sources.rows()
    val batch = sources.getSchema.createRowBatch()

    var id = 0L
    while (rows.nextBatch(batch)) {
      val column = batch.cols(index).asInstanceOf[LongColumnVector].vector
      for (i <- 0 until column.length) {
        id = math.max(column(i), id)
      }
    }
    rows.close()
    id.toInt
  }

  private def getReader(path: Path): Reader = {
    val readerOptions = OrcFile.readerOptions(new Configuration)
    OrcFile.createReader(path, readerOptions)
  }

  private def getWriter(location: String, schema: TypeDescription): Writer = {
    val writerOptions = OrcFile.writerOptions(new Configuration())
    writerOptions.setSchema(schema)
    writerOptions.compress(CompressionKind.SNAPPY)
    OrcFile.createWriter(new Path(location), writerOptions)
  }

  private def flush(writer: Writer, batch: VectorizedRowBatch): Unit = {
    if (batch.size != 0) {
      writer.addRowBatch(batch)
      batch.reset()
    }
  }

  private def listFiles(folder: Path): util.ArrayList[Path] = {
    val files = this.fileSystem.listFiles(folder, false)
    val fileLists = new util.ArrayList[Path]()
    while (files.hasNext) {
      fileLists.add(files.next().getPath)
    }
    fileLists
  }

  private def getSegmentPartLocation: String = {
    this.segmentFolder + "/part-" +  UUID.randomUUID().toString + "-" + System.currentTimeMillis() + ".orc"
  }
  private def shouldMerge(): Boolean = {
    //TODO: How often should a merged be performed? N is chosen arbitrarily
    this.batchesSinceLastMerge += 1
    this.batchesSinceLastMerge == 500
  }

  private def merge(folder: String, prefix: String): Unit = {
    val files = listFiles(new Path(this.location))
    files.removeIf((path: Path) => ! path.getName.startsWith(prefix))
    merge(files, folder + "/" + prefix + ".orc")
  }

  private def merge(inputPaths: util.ArrayList[Path], output: String): Unit = {
    //NOTE: merge assumes all inputs share the same schema
    val outputMerge = new Path(output + "_merge")
    val configuration = OrcFile.writerOptions(new Configuration())
    OrcFile.mergeFiles(outputMerge, configuration, inputPaths)

    val inputPathsIter = inputPaths.iterator()
    while (inputPathsIter.hasNext) {
      this.fileSystem.delete(inputPathsIter.next(), false)
    }
    this.fileSystem.rename(outputMerge, new Path(output))
  }

  private def readBytes(source: BytesColumnVector, row: Int): Array[Byte] = {
    val destination = Array.fill[Byte](source.length(row))(0)
    System.arraycopy(source.vector(row), source.start(row), destination, 0, source.length(row))
    destination
  }

  /** Instance Variables **/
  private var batchesSinceLastMerge = 0
  private val segmentFolder = this.location + "/segment"
  private val segmentFolderPath = new Path(this.location + "/segment")
  private val fileSystem = segmentFolderPath.getFileSystem(new Configuration())
  private var sparkSession: SparkSession = _
  private val segmentSchema = TypeDescription.createStruct()
    .addField("gid", TypeDescription.createInt())
    .addField("start_time", TypeDescription.createTimestamp())
    .addField("end_time", TypeDescription.createTimestamp())
    .addField("mtid", TypeDescription.createInt())
    .addField("model", TypeDescription.createBinary())
    .addField("gaps", TypeDescription.createBinary())
  private val segmentSparkSchema = StructType(Seq(
    StructField("gid", IntegerType, nullable = false),
    StructField("start_time", TimestampType, nullable = false),
    StructField("end_time", TimestampType, nullable = false),
    StructField("mtid", IntegerType, nullable = false),
    StructField("model", BinaryType, nullable = false),
    StructField("gaps", BinaryType, nullable = false)))
}
