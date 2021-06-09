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
import dk.aau.modelardb.core.{Dimensions, SegmentGroup}
import dk.aau.modelardb.engines.spark.Spark
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.example.GroupWriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetWriter}
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.{ColumnIOFactory, MessageColumnIO, RecordReader}
import org.apache.parquet.schema.{MessageType, PrimitiveType, Type}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.h2.table.TableFilter

import java.io.FileNotFoundException
import java.sql.Timestamp
import java.util
import scala.collection.JavaConverters._

class ParquetStorage(rootFolder: String) extends FileStorage(rootFolder) {
  /** Instance Variables **/
  private val segmentSchema = new MessageType("segment",
    new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "gid" ),
    new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "start_time"),
    new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "end_time"),
    new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "mtid" ),
    new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "model"),
    new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "gaps"))

  /** Public Methods **/
  //Storage
  def storeTimeSeries(timeSeriesGroups: Array[dk.aau.modelardb.core.TimeSeriesGroup]): Unit = {
    val columns = new util.ArrayList[Type]()
    columns.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "tid"))
    columns.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.FLOAT, "scaling_factor"))
    columns.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "sampling_interval"))
    columns.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "gid"))

    val dimensionTypes = dimensions.getTypes
    for (dimi <- dimensions.getColumns.zipWithIndex) {
      dimensionTypes(dimi._2) match {
        case Dimensions.Types.TEXT => columns.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, dimi._1))
        case Dimensions.Types.INT => columns.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, dimi._1))
        case Dimensions.Types.LONG => columns.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, dimi._1))
        case Dimensions.Types.FLOAT => columns.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.FLOAT, dimi._1))
        case Dimensions.Types.DOUBLE => columns.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, dimi._1))
      }
    }
    val schema = new MessageType("time_series", columns)
    val writer = getWriter(new Path(this.rootFolder + "/time_series.parquet_new"), schema)
    for (tsg <- timeSeriesGroups) {
      for (ts <- tsg.getTimeSeries) {
        val group = new SimpleGroup(schema)
        group.add(0, ts.tid)
        group.add(1, ts.scalingFactor)
        group.add(2, ts.samplingInterval)
        group.add(3, tsg.gid)
        for (mi <- dimensions.get(ts.source).zipWithIndex) {
          dimensionTypes(mi._2) match {
            case Dimensions.Types.TEXT => group.add(4 + mi._2, mi._1.toString)
            case Dimensions.Types.INT => group.add(4 + mi._2, mi._1.asInstanceOf[Int])
            case Dimensions.Types.LONG => group.add(4 + mi._2, mi._1.asInstanceOf[Long])
            case Dimensions.Types.FLOAT => group.add(4 + mi._2, mi._1.asInstanceOf[Float])
            case Dimensions.Types.DOUBLE => group.add(4 + mi._2, mi._1.asInstanceOf[Double])
          }
        }
        writer.write(group)
      }
    }
    writer.close()
    merge("time_series.parquet", "time_series.parquet", "time_series.parquet_new")
  }

  override def getTimeSeries: util.HashMap[Integer, Array[Object]] = {
    val columnsInNormalizedDimensions = dimensions.getColumns.length
    val timeSeriesInStorage = new util.HashMap[Integer, Array[Object]]()
    val timeSeries = try {
      getReader(new Path(this.rootFolder + "/time_series.parquet"))
    } catch {
      case _: FileNotFoundException => return timeSeriesInStorage
    }

    var pages = timeSeries.readNextRowGroup()
    val schema = timeSeries.getFooter.getFileMetaData.getSchema
    val columnIO = new ColumnIOFactory().getColumnIO(schema)
    while (pages != null) {
      val recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))
      for (_ <- 0 until pages.getRowCount.toInt) {
        //The metadata is stored as (Sid => Scaling, Resolution, Gid, Dimensions)
        val group = recordReader.read()
        val metadata = new util.ArrayList[Object]()
        metadata.add(group.getFloat(1, 0).asInstanceOf[Object])
        metadata.add(group.getInteger(2, 0).asInstanceOf[Object])
        metadata.add(group.getInteger(3, 0).asInstanceOf[Object])

        //Dimensions
        var column = 4
        val dimensionTypes = dimensions.getTypes
        while(column < columnsInNormalizedDimensions + 4) {
          dimensionTypes(column - 4) match {
            case Dimensions.Types.TEXT => metadata.add(group.getString(column,0))
            case Dimensions.Types.INT => metadata.add(group.getInteger(column,0).asInstanceOf[Object])
            case Dimensions.Types.LONG => metadata.add(group.getLong(column,0).asInstanceOf[Object])
            case Dimensions.Types.FLOAT => metadata.add(group.getFloat(column,0).asInstanceOf[Object])
            case Dimensions.Types.DOUBLE => metadata.add(group.getDouble(column,0).asInstanceOf[Object])
          }
          column += 1
        }
        timeSeriesInStorage.put(group.getInteger(0, 0), metadata.toArray)
      }
      pages = timeSeries.readNextRowGroup()
    }
    timeSeries.close()
    timeSeriesInStorage
  }

  override def storeModelTypes(modelsToInsert: java.util.HashMap[String,Integer]): Unit = {
    val schema = new MessageType("model_type",
      new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "mid"),
      new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "name"))

    val writer = getWriter(new Path(this.rootFolder + "/model_type.parquet_new"), schema)
    for ((k, v) <- modelsToInsert.asScala) {
      val group = new SimpleGroup(schema)
      group.add(0, v.intValue())
      group.add(1, k)
      writer.write(group)
    }
    writer.close()
    merge("model_type.parquet", "model_type.parquet", "model_type.parquet_new")
  }

  override def getModelTypes: util.HashMap[String, Integer] = {
    val modelsInStorage = new util.HashMap[String, Integer]()
    val modelTypes = try {
      getReader(new Path(this.rootFolder + "/model_type.parquet"))
    } catch {
      case _: FileNotFoundException => return modelsInStorage
    }

    var pages = modelTypes.readNextRowGroup()
    val schema = modelTypes.getFooter.getFileMetaData.getSchema
    val columnIO = new ColumnIOFactory().getColumnIO(schema)
    while (pages != null) {
      val recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))
      for (_ <- 0 until pages.getRowCount.toInt) {
        val group = recordReader.read()
        val mid = group.getInteger(0, 0)
        val name = group.getString(1, 0)
        modelsInStorage.put(name, mid)
      }
      pages = modelTypes.readNextRowGroup()
    }
    modelTypes.close()
    modelsInStorage
  }

  //H2Storage
  override def storeSegmentGroups(segmentGroups: Array[SegmentGroup], size: Int): Unit = {
    val writer = getWriter(new Path(getSegmentPartPath(".parquet")), this.segmentSchema)
    for (segmentGroup <- segmentGroups.take(size)) {
      val group = new SimpleGroup(this.segmentSchema)
      group.add(0, segmentGroup.gid)
      group.add(1, segmentGroup.startTime)
      group.add(2, segmentGroup.endTime)
      group.add(3, segmentGroup.mtid)
      group.add(4, Binary.fromConstantByteArray(segmentGroup.model))
      group.add(5, Binary.fromConstantByteArray(segmentGroup.offsets))
      writer.write(group)
    }
    writer.close()

    if (shouldMerge()) {
      merge(new Path(this.segmentFolder + "/segment.parquet"), listFiles(this.segmentFolderPath))
    }
  }

  override def getSegmentGroups(filter: TableFilter): Iterator[SegmentGroup] = {
    Static.warn("ModelarDB: projection and predicate push-down is not yet implemented")
    new Iterator[SegmentGroup] {
      /** Instance Variables **/
      private val segmentFiles = listFiles(segmentFolderPath).iterator()
      private var segmentFile: ParquetFileReader = _
      private var columnIO: MessageColumnIO = _
      private var pages: PageReadStore = _
      private var recordReader: RecordReader[Group] = _
      private var rowCount: Long = _
      private var rowIndex: Long = _
      nextFile()

      /** Public Methods **/
      override def hasNext: Boolean = {
        //The current batch contain additional rows
        if (this.rowIndex < this.rowCount) {
          return true
        }

        //The current file contain additional batches
        this.pages = this.segmentFile.readNextRowGroup()
        if (this.pages != null) {
          this.recordReader = this.columnIO.getRecordReader(this.pages, new GroupRecordConverter(segmentSchema))
          this.rowCount = this.pages.getRowCount
          this.rowIndex = 0L
          return true
        }

        //There are more files to read
        this.segmentFile.close()
        if (this.segmentFiles.hasNext) {
          nextFile()
          return true
        }

        //All of the data in the file have been read
        false
      }

      override def next(): SegmentGroup = {
        val simpleGroup = this.recordReader.read
        val gid = simpleGroup.getInteger(0, 0)
        val startTime = simpleGroup.getLong(1, 0)
        val endTime = simpleGroup.getLong(2, 0)
        val mtid = simpleGroup.getInteger(3, 0)
        val model = simpleGroup.getBinary(4, 0).getBytesUnsafe
        val gaps = simpleGroup.getBinary(5, 0).getBytesUnsafe
        this.rowIndex += 1
        new SegmentGroup(gid, startTime, endTime, mtid, model, gaps)
      }

      /** Private Methods **/
      private def nextFile(): Unit = {
        this.segmentFile = getReader(segmentFiles.next())
        this.columnIO = new ColumnIOFactory().getColumnIO(segmentSchema)
        this.pages = segmentFile.readNextRowGroup()
        this.recordReader = columnIO.getRecordReader(this.pages, new GroupRecordConverter(segmentSchema))
        this.rowCount = this.pages.getRowCount
        this.rowIndex = 0L
      }
    }
  }

  //SparkStorage
  override def storeSegmentGroups(sparkSession: SparkSession, rdd: RDD[Row]): Unit = {
    if ( ! shouldMerge) {
      //Add new Parquet files for this batch to the existing folder
      sparkSession.createDataFrame(rdd, Spark.segmentFileSchema)
        .write.mode(SaveMode.Append).parquet(this.segmentFolder)
    } else {
      //Writes new Parquet files with the segment on disk and from this batch
      val rddDF = sparkSession.createDataFrame(rdd, Spark.segmentFileSchema)
        .union(sparkSession.read.schema(Spark.segmentFileSchema).parquet(this.segmentFolder))
      val newSegmentFolder = new Path(this.rootFolder + "/segment_new")
      rddDF.write.parquet(newSegmentFolder.toString)

      //Overwrite the old segment folder with the new segment folder
      this.fileSystem.delete(this.segmentFolderPath, true)
      this.fileSystem.rename(newSegmentFolder, this.segmentFolderPath)
    }
  }

  override def getSegmentGroups(sparkSession: SparkSession, filters: Array[Filter]): RDD[Row] = {
    val df = sparkSession.read.parquet(this.rootFolder + "/segment/segment.parquet")
    pushDownSparkFilters(df, filters).rdd.map(row => Row(row.getInt(0), new Timestamp(row.getLong(1)),
      new Timestamp(row.getLong(2)), row.getInt(3), row.getAs[Array[Byte]](4), row.getAs[Array[Byte]](5)))
  }

  /** Protected Methods **/
  protected override def getMaxID(index: Int): Int = {
    val reader = try {
      getReader(new Path(this.rootFolder + "/time_series.parquet"))
    } catch {
      case _: FileNotFoundException => return 0
    }
    var id = 0
    val schema = reader.getFooter.getFileMetaData.getSchema
    val columnIO = new ColumnIOFactory().getColumnIO(schema)
    var pages = reader.readNextRowGroup()
    while (pages != null) {
      val recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))
      for (_ <- 0 until pages.getRowCount.toInt) {
        val simpleGroup = recordReader.read.asInstanceOf[SimpleGroup]
        id = Math.max(id, simpleGroup.getInteger(index, 0))
      }
      pages = reader.readNextRowGroup()
    }
    reader.close()
    id
  }

  protected override def merge(outputFilePath: Path, inputPaths: util.ArrayList[Path]): Unit = {
    //TODO: determine why merge throws UnsupportedOperationException for model types and time series but not segment
    //NOTE: merge assumes all inputs share the same schema
    val inputPathsScala = inputPaths.asScala
    val segmentFile = getReader(inputPaths.get(0))
    segmentFile.close()

    //Write the new file
    val outputMerge  = new Path(outputFilePath + "_merge")
    val writer = getWriter(outputMerge, this.segmentSchema)
    for (inputPath <- inputPathsScala) {
      val segmentFile = getReader(inputPath)
      val columnIO = new ColumnIOFactory().getColumnIO(schema)
      var pages = segmentFile.readNextRowGroup()

      while (pages != null) {
        val recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))
        for (_ <- 0L until pages.getRowCount) {
          writer.write(recordReader.read)
        }
        pages = segmentFile.readNextRowGroup()
      }
      segmentFile.close()
    }
    writer.close()

    //Delete the old files
    for (inputPath <- inputPathsScala) {
      this.fileSystem.delete(inputPath, false)
    }
    this.fileSystem.rename(outputMerge, outputFilePath)
    this.batchesSinceLastMerge = 0
  }

  /** Private Methods **/
  private def getWriter(parquetFile: Path, schema: MessageType): ParquetWriter[Group] = {
    new ParquetWriterBuilder(parquetFile)
      .withType(schema).withCompressionCodec(CompressionCodecName.SNAPPY).build()
  }

  private def getReader(path: Path) = {
    ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()))
  }
}

private class ParquetWriterBuilder(path: Path) extends ParquetWriter.Builder[Group, ParquetWriterBuilder](path) {
  /** Instance Variables * */
  private var messageType: MessageType = _

  /** Public Methods **/
  def withType(messageType: MessageType): ParquetWriterBuilder = {
    this.messageType = messageType
    this
  }
  override protected def self: ParquetWriterBuilder = this
  override protected def getWriteSupport(conf: Configuration): WriteSupport[Group] = {
    GroupWriteSupport.setSchema(this.messageType, conf)
    new GroupWriteSupport()
  }
}