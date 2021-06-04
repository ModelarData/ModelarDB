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

import java.util
import java.sql.Timestamp
import java.io.FileNotFoundException
import dk.aau.modelardb.core.utility.Static
import dk.aau.modelardb.core.SegmentGroup
import dk.aau.modelardb.engines.spark.Spark
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.sources.Filter
import org.h2.table.TableFilter

class ParquetStorage(rootFolder: String) extends FileStorage(rootFolder) {

  /** Public Methods **/
  //Storage
  def storeTimeSeries(timeSeriesGroups: Array[dk.aau.modelardb.core.TimeSeriesGroup]): Unit = {
    //TODO: Implement
  }

  override def getTimeSeries: util.HashMap[Integer, Array[Object]] = {
    val timeSeriesInStorage = new util.HashMap[Integer, Array[Object]]()
    val timeSeries = try {
      getReader(new Path(this.rootFolder + "/time_series.parquet"))
    } catch {
      case _: FileNotFoundException => return timeSeriesInStorage
    }

    //The metadata is stored as (Sid => Scaling, Resolution, Gid, Dimensions)
    var pages = timeSeries.readNextRowGroup()
    val schema = timeSeries.getFooter.getFileMetaData.getSchema
    val columnIO = new ColumnIOFactory().getColumnIO(schema)
    while (pages != null) {
      val recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))
      for (_ <- 0 until pages.getRowCount.toInt) {
        val group = recordReader.read()
        val metadata = new util.ArrayList[Object]()
        metadata.add(group.getDouble(1, 0).toFloat.asInstanceOf[Object])
        metadata.add(group.getInteger(2, 0).asInstanceOf[Object])
        metadata.add(group.getInteger(3, 0).asInstanceOf[Object])
        //TODO: Add support for dimensions
        timeSeriesInStorage.put(group.getInteger(0, 0), metadata.toArray)
      }
      pages = timeSeries.readNextRowGroup()
    }
    timeSeries.close()
    timeSeriesInStorage
  }

  override def storeModelTypes(modelsToInsert: java.util.HashMap[String,Integer]): Unit = {
    //TODO: Implement
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
  override def storeSegmentGroups(segmentGroups: Array[SegmentGroup], size: Int): Unit = ???

  override def getSegmentGroups(filter: TableFilter): Iterator[SegmentGroup] = {
    Static.warn("ModelarDB: projection and predicate push-down is not yet implemented")
    new Iterator[SegmentGroup] {
      /** Instance Variables **/
      private val segmentFiles = listFiles(segmentFolderPath).iterator()
      private var segmentFile = getReader(segmentFiles.next())
      private val schema = segmentFile.getFooter.getFileMetaData.getSchema
      private val columnIO = new ColumnIOFactory().getColumnIO(schema)
      private var pages = segmentFile.readNextRowGroup()
      private var recordReader = columnIO.getRecordReader(this.pages, new GroupRecordConverter(schema))
      private var rowCount = this.pages.getRowCount
      private var rowIndex = 0L

      /** Public Methods **/
      override def hasNext: Boolean = {
        //The current batch contain additional rows
        if (this.rowIndex < this.rowCount) {
          return true
        }

        //The current file contain additional batches
        this.pages = segmentFile.readNextRowGroup()
        if (this.pages != null) {
          this.rowCount = this.pages.getRowCount
          this.recordReader = columnIO.getRecordReader(this.pages, new GroupRecordConverter(schema))
          this.rowIndex = 0L
          return true
        }

        //There are more files to read
        if (this.segmentFiles.hasNext) {
          this.segmentFile = getReader(this.segmentFiles.next())
          this.rowIndex = 0
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
    val reader = getReader(new Path(this.rootFolder + "/time_series.parquet"))
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

  protected override def merge(inputPaths: util.ArrayList[Path], output: String): Unit = ???

  /** Private Methods **/
  private def getReader(path: Path): ParquetFileReader = {
    ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()))
  }
}