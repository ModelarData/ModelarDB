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


import java.io.FileNotFoundException
import java.sql.Timestamp
import java.util
import java.util.UUID
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.h2.table.TableFilter

import org.apache.hadoop.hive.ql.exec.vector._
import org.apache.orc.{CompressionKind, OrcFile, Reader, TypeDescription, Writer}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{Row, SparkSession}

import dk.aau.modelardb.core.utility.{Pair, ValueFunction}
import dk.aau.modelardb.core.{Dimensions, SegmentGroup, TimeSeriesGroup}

import scala.collection.JavaConverters._

class ORCStorage(rootFolder: String) extends FileStorage(rootFolder) {
  /** Instance Variables **/
  private val segmentSchema = TypeDescription.createStruct()
    .addField("gid", TypeDescription.createInt())
    .addField("start_time", TypeDescription.createTimestamp())
    .addField("end_time", TypeDescription.createTimestamp())
    .addField("mtid", TypeDescription.createInt())
    .addField("model", TypeDescription.createBinary())
    .addField("gaps", TypeDescription.createBinary())

  /** Public Methods **/
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
    val sourceOut = getWriter(this.rootFolder + "/time_series_new.orc", schema)
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
    merge(this.rootFolder, "time_series")

    //Extracts all metadata for the sources in storage
    val sourceIn = getReader(new Path(this.rootFolder + "/time_series.orc"))
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
      val modelIn = getReader(new Path(this.rootFolder + "/model_type.orc"))
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
    val modelOut = getWriter(this.rootFolder + "/model_type.orc_new", schema)
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
    merge(this.rootFolder, "model_type")
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
    //TODO: is it possible to share implementation of Iterator[SegmentGroup] from different storage?
    new Iterator[SegmentGroup] {
      /** Instance Variables **/
      //Reads all non-hidden files in the folder so _SUCCESS is included, but it just treated as an empty ORC file
      private val segmentFiles = listFiles(segmentFolderPath).iterator()
      private var segmentFile = getReader(segmentFiles.next())
      private val rows = segmentFile.rows()
      private val batch = segmentFile.getSchema.createRowBatch()
      private var rowCount = batch.size
      private var rowIndex = 0

      /** Public Methods **/
      override def hasNext: Boolean = {
        //The current batch contain additional rows
        if (this.rowIndex < this.rowCount) {
          return true
        }

        //The current file contain additional batches
        if (rows.nextBatch(batch)) {
          this.rowCount = this.batch.size
          this.rowIndex = 0
          return hasNext
        }

        //There are more files to read
        if (this.segmentFiles.hasNext) {
          this.segmentFile = getReader(this.segmentFiles.next())
          this.rowIndex = 0
          return true
        }

        //All of the data in the file and all files have been read
        this.rows.close()
        this.segmentFile.close()
        false
      }

      override def next(): SegmentGroup = {
        val gid = this.batch.cols(0).asInstanceOf[LongColumnVector].vector(this.rowIndex).toInt
        val startTime = this.batch.cols(1).asInstanceOf[LongColumnVector].vector(this.rowIndex)
        val endTime = this.batch.cols(2).asInstanceOf[LongColumnVector].vector(this.rowIndex)
        val mtid = this.batch.cols(3).asInstanceOf[LongColumnVector].vector(this.rowIndex).toInt
        val model = readBytes(this.batch.cols(4).asInstanceOf[BytesColumnVector], this.rowIndex)
        val gaps = readBytes(this.batch.cols(5).asInstanceOf[BytesColumnVector], this.rowIndex)
        this.rowIndex += 1
        new SegmentGroup(gid, startTime, endTime, mtid, model, gaps)
      }
    }
  }

  //SparkStorage
  override def storeSegmentGroups(sparkSession: SparkSession, rdd: RDD[Row]): Unit = {
    /*
    import org.apache.spark.sql.types._
    val segmentSparkSchema = StructType(Seq(
      StructField("gid", IntegerType, nullable = false),
      StructField("start_time", TimestampType, nullable = false),
      StructField("end_time", TimestampType, nullable = false),
      StructField("mtid", IntegerType, nullable = false),
      StructField("model", BinaryType, nullable = false),
      StructField("gaps", BinaryType, nullable = false)))

    if ( ! shouldMerge) {
      //Add new ORC files for this batch to the existing folder
      this.sparkSession.createDataFrame(rdd, segmentSparkSchema)
        .write.mode(SaveMode.Append).orc(this.segmentFolder)
    } else {
      //Writes new ORC files with the segment on disk and from this batch
      val rddDF = this.sparkSession.createDataFrame(rdd, segmentSparkSchema)
        .union(this.sparkSession.read.schema(segmentSparkSchema).orc(this.segmentFolder))
      val newSegmentFolder = new Path(this.rootFolder + "/segment_new")
      rddDF.write.orc(newSegmentFolder.toString)

      //Overwrite the old segment folder with the new segment folder
      this.fileSystem.delete(this.segmentFolderPath, true)
      this.fileSystem.rename(newSegmentFolder, this.segmentFolderPath)
    }
    */
  }

  override def getSegmentGroups(sparkSession: SparkSession, filters: Array[Filter]): RDD[Row] = {
    sparkSession.read.orc(this.rootFolder + "/segment").rdd
  }

  /** Protected Methods **/
  protected override def getMaxID(index: Int): Int = {
    val sources = try {
      getReader(new Path(this.rootFolder + "/time_series.orc"))
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

  protected override def merge(inputPaths: util.ArrayList[Path], output: String): Unit = {
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

  /** Private Methods **/
  private def getReader(path: Path): Reader = {
    val readerOptions = OrcFile.readerOptions(new Configuration)
    OrcFile.createReader(path, readerOptions)
  }

  private def getWriter(orcFile: String, schema: TypeDescription): Writer = {
    val writerOptions = OrcFile.writerOptions(new Configuration())
    writerOptions.setSchema(schema)
    writerOptions.compress(CompressionKind.SNAPPY)
    OrcFile.createWriter(new Path(orcFile), writerOptions)
  }

  private def flush(writer: Writer, batch: VectorizedRowBatch): Unit = {
    if (batch.size != 0) {
      writer.addRowBatch(batch)
      batch.reset()
    }
  }

  private def getSegmentPartLocation: String = {
    this.segmentFolder + "/part-" +  UUID.randomUUID().toString + "-" + System.currentTimeMillis() + ".orc"
  }

  private def readBytes(source: BytesColumnVector, row: Int): Array[Byte] = {
    val destination = Array.fill[Byte](source.length(row))(0)
    System.arraycopy(source.vector(row), source.start(row), destination, 0, source.length(row))
    destination
  }
}