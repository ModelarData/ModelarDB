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
import dk.aau.modelardb.engines.spark.Spark
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector._
import org.apache.orc.{CompressionKind, OrcFile, Reader, RecordReader, TypeDescription, Writer}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.h2.table.TableFilter

import java.io.FileNotFoundException
import java.sql.Timestamp
import scala.collection.mutable

class ORCStorage(rootFolder: String, offset: Int) extends FileStorage(rootFolder, offset) {
  /** Instance Variables **/
  private val segmentGroupSchema = TypeDescription.createStruct()
    .addField("gid", TypeDescription.createInt())
    .addField("start_time", TypeDescription.createTimestamp())
    .addField("end_time", TypeDescription.createTimestamp())
    .addField("mtid", TypeDescription.createInt())
    .addField("model", TypeDescription.createBinary())
    .addField("gaps", TypeDescription.createBinary())

  /** Protected Methods **/
  //FileStorage
  protected override def getFileSuffix: String = ".orc"

  protected override def getMaxID(columnName: String, timeSeriesFilePath: Path): Int = {
    val fieldIndex = columnName match {
      case "tid" => 0
      case "gid" => 3
      case _ => throw new IllegalArgumentException("ModelarDB: unable to get the maximum id for column " + columnName)
    }
    val sources = try {
      getReader(timeSeriesFilePath)
    } catch {
      case _: FileNotFoundException => return 0
    }
    val rows = sources.rows()
    val batch = sources.getSchema.createRowBatch()

    var id = 0L
    while (rows.nextBatch(batch)) {
      val column = batch
        .cols(fieldIndex)
        .asInstanceOf[LongColumnVector]
        .vector
      for (i <- 0 until column.length) {
        id = math.max(column(i), id)
      }
    }
    rows.close()
    id.toInt + offset
  }

  protected override def mergeFiles(outputFilePath: Path, inputFilesPaths: mutable.ArrayBuffer[Path]): Unit = {
    //NOTE: merge assumes all inputs share the same schema
    val inputPathsScala = inputFilesPaths
    val segmentFile = getReader(inputFilesPaths(0))
    val schema = segmentFile.getSchema
    segmentFile.close()

    //Write the new file
    val writer = getWriter(outputFilePath, schema)
    for (inputPath <- inputPathsScala) {
      val segmentFile = getReader(inputPath)
      val rows = segmentFile.rows()
      val batch = segmentFile.getSchema.createRowBatch()
      while (rows.nextBatch(batch)) {
        writer.addRowBatch(batch)
      }
      rows.close()
      segmentFile.close()
    }
    writer.close()
  }

  override protected def writeTimeSeriesFile(timeSeriesGroups: Array[TimeSeriesGroup], timeSeriesFilePath: Path): Unit = {
    val schema = TypeDescription.createStruct()
      .addField("tid", TypeDescription.createInt())
      .addField("scaling_factor", TypeDescription.createFloat())
      .addField("sampling_interval", TypeDescription.createInt())
      .addField("gid", TypeDescription.createInt())

    val dimensionTypes = dimensions.getTypes
    for (dimi <- dimensions.getColumns.zipWithIndex) {
      dimensionTypes(dimi._2) match {
        case Dimensions.Types.TEXT => schema.addField(dimi._1, TypeDescription.createString())
        case Dimensions.Types.INT => schema.addField(dimi._1, TypeDescription.createInt())
        case Dimensions.Types.LONG => schema.addField(dimi._1, TypeDescription.createLong())
        case Dimensions.Types.FLOAT => schema.addField(dimi._1, TypeDescription.createFloat())
        case Dimensions.Types.DOUBLE => schema.addField(dimi._1, TypeDescription.createDouble())
      }
    }
    val source = getWriter(timeSeriesFilePath, schema)
    val batch = source.getSchema.createRowBatch()

    for (tsg <- timeSeriesGroups) {
      for (ts <- tsg.getTimeSeries) {
        val row = { batch.size += 1; batch.size - 1 } //batch.size++
        batch.cols(0).asInstanceOf[LongColumnVector].vector(row) = ts.tid
        batch.cols(1).asInstanceOf[DoubleColumnVector].vector(row) = ts.scalingFactor
        batch.cols(2).asInstanceOf[LongColumnVector].vector(row) = ts.samplingInterval
        batch.cols(3).asInstanceOf[LongColumnVector].vector(row) = tsg.gid
        for (mi <- dimensions.get(ts.source).zipWithIndex) {
          dimensionTypes(mi._2) match {
            case Dimensions.Types.TEXT => batch.cols(4 + mi._2).asInstanceOf[BytesColumnVector].setVal(row, mi._1.toString.getBytes)
            case Dimensions.Types.INT => batch.cols(4 + mi._2).asInstanceOf[LongColumnVector].vector(row) = mi._1.asInstanceOf[Int]
            case Dimensions.Types.LONG => batch.cols(4 + mi._2).asInstanceOf[LongColumnVector].vector(row) = mi._1.asInstanceOf[Long]
            case Dimensions.Types.FLOAT => batch.cols(4 + mi._2).asInstanceOf[DoubleColumnVector].vector(row) = mi._1.asInstanceOf[Float]
            case Dimensions.Types.DOUBLE => batch.cols(4 + mi._2).asInstanceOf[DoubleColumnVector].vector(row) = mi._1.asInstanceOf[Double]
          }
        }
        flushIfNecessary(source, batch)
      }
    }
    flush(source, batch)
    source.close()
  }

  override protected def readTimeSeriesFile(timeSeriesFilePath: Path): mutable.HashMap[Integer, Array[Object]] = {
    val columnsInNormalizedDimensions = dimensions.getColumns.length
    val timeSeriesInStorage = mutable.HashMap[Integer, Array[Object]]()
    val timeSeries = getReader(timeSeriesFilePath)

    val rows = timeSeries.rows()
    val batch = timeSeries.getSchema.createRowBatch()
    while (rows.nextBatch(batch)) {
      for (row <- 0 until batch.size) {
        //The metadata is stored as (Sid => Scaling, Resolution, Gid, Dimensions)
        val metadata = mutable.ArrayBuffer[Object]()
        val sid = batch.cols(0).asInstanceOf[LongColumnVector].vector(row)
        metadata += batch.cols(1).asInstanceOf[DoubleColumnVector].vector(row).toFloat.asInstanceOf[Object]
        metadata += batch.cols(2).asInstanceOf[LongColumnVector].vector(row).toInt.asInstanceOf[Object]
        metadata += batch.cols(3).asInstanceOf[LongColumnVector].vector(row).toInt.asInstanceOf[Object]

        //Dimensions
        var column = 4
        val dimensionTypes = dimensions.getTypes
        while(column < columnsInNormalizedDimensions + 4) {
          dimensionTypes(column - 4) match {
            case Dimensions.Types.TEXT => metadata += batch.cols(column).asInstanceOf[BytesColumnVector].vector(row)
            case Dimensions.Types.INT => metadata += batch.cols(column).asInstanceOf[LongColumnVector].vector(row).toInt.asInstanceOf[Object]
            case Dimensions.Types.LONG => metadata += batch.cols(column).asInstanceOf[LongColumnVector].vector(row).asInstanceOf[Object]
            case Dimensions.Types.FLOAT => metadata += batch.cols(column).asInstanceOf[DoubleColumnVector].vector(row).toFloat.asInstanceOf[Object]
            case Dimensions.Types.DOUBLE => metadata += batch.cols(column).asInstanceOf[DoubleColumnVector].vector(row).asInstanceOf[Object]
          }
          column += 1
        }
        timeSeriesInStorage.put(sid.toInt, metadata.toArray)
      }
      rows.close()
    }
    timeSeries.close()
    timeSeriesInStorage
  }

  override protected def writeModelTypeFile(modelsToInsert: mutable.HashMap[String,Integer], modelTypeFilePath: Path): Unit = {
    val schema = TypeDescription.createStruct()
      .addField("mid", TypeDescription.createInt())
      .addField("name", TypeDescription.createString())
    val modelTypes = getWriter(modelTypeFilePath, schema)
    val batch = modelTypes.getSchema.createRowBatch()

    for ((k, v) <- modelsToInsert) {
      val row = { batch.size += 1; batch.size - 1 } //batch++
      batch.cols(0).asInstanceOf[LongColumnVector].vector(row) = v.intValue()
      batch.cols(1).asInstanceOf[BytesColumnVector].setVal(row, k.getBytes)
      flushIfNecessary(modelTypes, batch)
    }
    flush(modelTypes, batch)
    modelTypes.close()
  }

  override protected def readModelTypeFile(modelTypeFilePath: Path): mutable.HashMap[String, Integer] = {
    val modelsInStorage = mutable.HashMap[String, Integer]()
    val modelTypes = try {
      getReader(modelTypeFilePath)
    } catch {
      case _: FileNotFoundException => return modelsInStorage
    }

    val rows = modelTypes.rows()
    val batch = modelTypes.getSchema.createRowBatch()
    while (rows.nextBatch(batch)) {
      for (row <- 0 until batch.size) {
        val mid = batch.cols(0).asInstanceOf[LongColumnVector].vector(row).toInt
        val cp = batch.cols(1).asInstanceOf[BytesColumnVector].toString(row)
        modelsInStorage.put(cp, mid)
      }
    }
    modelsInStorage
  }

  //FileStorage - H2Storage
  override protected def writeSegmentGroupFile(segmentGroups: Array[SegmentGroup], size: Int, segmentGroupFilePath: Path): Unit = {
    val segments = getWriter(segmentGroupFilePath, this.segmentGroupSchema)
    val batch = segments.getSchema.createRowBatch()

    for (segmentGroup <- segmentGroups.take(size)) {
      val row = { batch.size += 1; batch.size - 1 }
      batch.cols(0).asInstanceOf[LongColumnVector].vector(row) = segmentGroup.gid
      batch.cols(1).asInstanceOf[TimestampColumnVector].set(row, new Timestamp(segmentGroup.startTime))
      batch.cols(2).asInstanceOf[TimestampColumnVector].set(row, new Timestamp(segmentGroup.endTime))
      batch.cols(3).asInstanceOf[LongColumnVector].vector(row) = segmentGroup.mtid
      batch.cols(4).asInstanceOf[BytesColumnVector].setVal(row, segmentGroup.model)
      batch.cols(5).asInstanceOf[BytesColumnVector].setVal(row, segmentGroup.offsets)
      flushIfNecessary(segments, batch)
    }
    flush(segments, batch)
    segments.close()
  }

  override protected def readSegmentGroupsFiles(filter: TableFilter, segmentGroupFiles: mutable.ArrayBuffer[Path]): Iterator[SegmentGroup] = {
    Static.warn("ModelarDB: projection and predicate push-down is not yet implemented")
    new Iterator[SegmentGroup] {
      /** Instance Variables **/
      private val segmentFiles = segmentGroupFiles.iterator
      private var segmentFile: Reader = _
      private var rows: RecordReader = _
      private var batch: VectorizedRowBatch = _
      private var rowCount: Int = _
      private var rowIndex: Int = _
      nextFile()

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
          return true
        }

        //There are more files to read
        this.rows.close()
        this.segmentFile.close()
        if (this.segmentFiles.hasNext) {
          nextFile()
          return true
        }

        //All of the data in the file and all files have been read
        false
      }

      override def next(): SegmentGroup = {
        val gid = this.batch.cols(0).asInstanceOf[LongColumnVector].vector(this.rowIndex).toInt
        val startTime = this.batch.cols(1).asInstanceOf[TimestampColumnVector].getTime(this.rowIndex)
        val endTime = this.batch.cols(2).asInstanceOf[TimestampColumnVector].getTime(this.rowIndex)
        val mtid = this.batch.cols(3).asInstanceOf[LongColumnVector].vector(this.rowIndex).toInt
        val model = readBytes(this.batch.cols(4).asInstanceOf[BytesColumnVector], this.rowIndex)
        val gaps = readBytes(this.batch.cols(5).asInstanceOf[BytesColumnVector], this.rowIndex)
        this.rowIndex += 1
        new SegmentGroup(gid, startTime, endTime, mtid, model, gaps)
      }

      /** Private Methods **/
      private def nextFile(): Unit = {
        this.segmentFile = getReader(segmentFiles.next())
        this.rows = this.segmentFile.rows()
        this.batch = this.segmentFile.getSchema.createRowBatch()
        this.rows.nextBatch(batch)
        this.rowCount = this.batch.size
        this.rowIndex = 0
      }
    }
  }

  //FileStorage - SparkStorage
  override protected def writeSegmentGroupsFolder(sparkSession: SparkSession, df: DataFrame, segmentGroupFolder: String): Unit = {
    df.write.mode(SaveMode.Append).orc(segmentGroupFolder)
  }

  override protected def readSegmentGroupsFolders(sparkSession: SparkSession, filters: Array[Filter],
                                                  segmentGroupFolders: mutable.ArrayBuffer[String]): DataFrame = {
    val segmentGroupFoldersIterator = segmentGroupFolders.iterator
    var df = sparkSession.read.orc(segmentGroupFoldersIterator.next())
    while (segmentGroupFoldersIterator.hasNext) {
      df = df.union(sparkSession.read.orc(segmentGroupFoldersIterator.next()))
    }
    Spark.applyFiltersToDataFrame(df, filters)
  }

  /** Private Methods **/
  private def getReader(orcFilePath: Path): Reader = {
    OrcFile.createReader(orcFilePath, OrcFile.readerOptions(new Configuration))
  }

  private def getWriter(orcFilePath: Path, schema: TypeDescription): Writer = {
    val writerOptions = OrcFile.writerOptions(new Configuration())
    writerOptions.setSchema(schema)
    writerOptions.compress(CompressionKind.SNAPPY)
    OrcFile.createWriter(orcFilePath, writerOptions)
  }

  private def flushIfNecessary(writer: Writer, batch: VectorizedRowBatch): Unit = {
    if (batch.size == batch.getMaxSize) {
      flush(writer, batch)
    }
  }

  private def flush(writer: Writer, batch: VectorizedRowBatch): Unit = {
    if (batch.size != 0) {
      writer.addRowBatch(batch)
      batch.reset()
    }
  }

  private def readBytes(source: BytesColumnVector, row: Int): Array[Byte] = {
    val destination = Array.fill[Byte](source.length(row))(0)
    System.arraycopy(source.vector(row), source.start(row), destination, 0, source.length(row))
    destination
  }
}