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
import java.sql.Timestamp
import java.util
import dk.aau.modelardb.core.utility.{Pair, Static, ValueFunction}
import dk.aau.modelardb.core.{Dimensions, SegmentGroup, Storage, TimeSeriesGroup}
import dk.aau.modelardb.engines.h2.H2Storage
import dk.aau.modelardb.engines.spark.SparkStorage
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession, sources}
import org.apache.spark.sql.sources.Filter
import org.h2.table.TableFilter

class ParquetStorage(folderPath: String) extends Storage with H2Storage with SparkStorage {

  /** Public Methods **/
  override def open(dimensions: Dimensions): Unit = {
    //The parquet files are opened as needed by ModelarDB
  }

  override def initialize(timeSeriesGroups: Array[TimeSeriesGroup],
                          derivedTimeSeries: util.HashMap[Integer, Array[Pair[String, ValueFunction]]],
                          dimensions: Dimensions, modelNames: Array[String]): Unit = {
    //NOTE: This data source is currently read only so new groups are an error
    if (timeSeriesGroups.nonEmpty) {
      throw new IllegalArgumentException("ModelarDB: ParquetStorage are read only")
    }

    //Extracts all metadata for the sources in storage
    val source = getParquetReader(this.folderPath + "/time_series.snappy.parquet")
    val sourcesInStorage = new util.HashMap[Integer, Array[Object]]()
    var pages = source.readNextRowGroup()
    var schema = source.getFooter.getFileMetaData.getSchema
    while (pages != null) {
      //The metadata is stored as (Sid => Scaling, Resolution, Gid, Dimensions)
      //The parquet file consist of sid category concrete entity gid resolution scaling type
      //HACK: Hardcoded to match the Parquet files exported from Cassandra
      val columnIO = new ColumnIOFactory().getColumnIO(schema)
      val recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))
      //for (_ <- 0 until pages.getRowCount.toInt) {
      var i = 0
      while (i < pages.getRowCount.toInt) {
        val group = recordReader.read()
        val metadata = new util.ArrayList[Object]()
        metadata.add(group.getDouble(1, 0).toFloat.asInstanceOf[Object])
        metadata.add(group.getInteger(2, 0).asInstanceOf[Object])
        metadata.add(group.getInteger(3, 0).asInstanceOf[Object])
        //TODO: Add support for dimensions
        sourcesInStorage.put(group.getInteger(0, 0), metadata.toArray)
        i += 1
      }
      pages = source.readNextRowGroup()
    }
    source.close()

    //Extracts all model names from storage
    val model = getParquetReader(this.folderPath + "/model_type.snappy.parquet")
    pages = model.readNextRowGroup()
    schema = model.getFooter.getFileMetaData.getSchema
    val modelsInStorage = new util.HashMap[String, Integer]()
    while (pages != null) {
      val columnIO = new ColumnIOFactory().getColumnIO(schema)
      val recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))
      for (_ <- 0 until pages.getRowCount.toInt) {
        val group = recordReader.read()
        val mid = group.getInteger(0, 0)
        val name = group.getString(1, 0)
        modelsInStorage.put(name, mid)
      }
      pages = model.readNextRowGroup()
    }
    model.close()

    //Initializes the storage caches
    super.initializeCaches(modelNames, dimensions, modelsInStorage, sourcesInStorage, derivedTimeSeries)
  }

  override def getMaxTid: Int = {
    getMaxID(0)
  }

  override def getMaxGid: Int = {
    getMaxID(3)
  }

  override def close(): Unit = {
    //NOTE: no files or connections are opened
  }

  //H2Storage
  override def storeSegmentGroups(segmentGroups: Array[SegmentGroup], size: Int): Unit = ???

  override def getSegmentGroups(filter: TableFilter): Iterator[SegmentGroup] = {
    getSegmentGroups
  }

  //SparkStorage
  override def open(ssb: SparkSession.Builder, dimensions: Dimensions): SparkSession = {
    this.sparkSession = ssb.getOrCreate()
    //TODO: Support reading segment as a folder instead of each file separately
    this.segment = this.sparkSession.read.parquet(folderPath + "/segment/segment.snappy.parquet")
    this.sparkSession
  }

  override def storeSegmentGroups(sparkSession: SparkSession, rdd: RDD[Row]): Unit = ???

  override def getSegmentGroups(sparkSession: SparkSession, filters: Array[Filter]): RDD[Row] = {
    var df = this.segment
    for (filter: Filter <- filters) {
      filter match {
        //Predicate push-down for gid using SELECT * FROM segment with GID = ? and gid IN (..)
        case sources.GreaterThan("gid", value: Int) => df = df.filter(s"sid > $value")
        case sources.GreaterThanOrEqual("gid", value: Int) => df = df.filter(s"sid >= $value")
        case sources.LessThan("gid", value: Int) => df = df.filter(s"sid < $value")
        case sources.LessThanOrEqual("gid", value: Int) => df = df.filter(s"sid <= $value")
        case sources.EqualTo("gid", value: Int) => df = df.filter(s"sid = $value")
        case sources.In("gid", value: Array[Any]) => df = df.filter(value.mkString("sid IN (", ",", ")"))

        //Predicate push-down for et using SELECT * FROM segment WHERE et <=> ?
        case sources.GreaterThan("et", value: Timestamp) => df.filter(s"end_time > '$value'")
        case sources.GreaterThanOrEqual("et", value: Timestamp) => df.filter(s"end_time >= '$value'")
        case sources.LessThan("et", value: Timestamp) => df.filter(s"end_time < '$value'")
        case sources.LessThanOrEqual("et", value: Timestamp) => df.filter(s"end_time <= '$value'")
        case sources.EqualTo("et", value: Timestamp) => df.filter(s"end_time = '$value'")

        //The predicate cannot be supported by the segment view so all we can do is inform the user
        case p => Static.warn("ModelarDB: unsupported predicate for ParquetStorage " + p, 120); null
      }
    }
    val rowsToRows = getRowsToRows
    df.rdd.map(rowsToRows)
  }

  /** Private Methods **/
  def getSegmentGroups: Iterator[SegmentGroup] = {
    Static.warn("ModelarDB: projection and predicate push-down is not yet implemented")
    //TODO: Support reading segment as a folder instead of each file separately
    val reader = getParquetReader(folderPath + "/segment/segment.snappy.parquet")
    val schema = reader.getFooter.getFileMetaData.getSchema
    val columnIO = new ColumnIOFactory().getColumnIO(schema)

    new Iterator[SegmentGroup] {
      /** Public Methods **/
      override def hasNext: Boolean = {
        //The current batch contain additional rows
        if (this.rowIndex < this.rowCount) {
          return true
        }

        //The current file contain additional batches
        this.pages = reader.readNextRowGroup()
        if (this.pages != null) {
          this.rowCount = this.pages.getRowCount
          this.recordReader = columnIO.getRecordReader(this.pages, new GroupRecordConverter(schema))
          this.rowIndex = 0L
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
        new SegmentGroup(gid, startTime, endTime, mtid, model, gaps)
      }

      /** Instance Variables **/
      private var pages = reader.readNextRowGroup()
      private var recordReader = columnIO.getRecordReader(this.pages, new GroupRecordConverter(schema))
      private var rowCount = this.pages.getRowCount
      private var rowIndex = 0L
    }
  }

  private def getParquetReader(stringPath: String): ParquetFileReader = {
    ParquetFileReader.open(HadoopInputFile.fromPath(new Path(stringPath), new Configuration()))
  }

  private def getMaxID(column: Int): Int = {
    val reader = getParquetReader(folderPath + "/time_series.snappy.parquet")
    var id = 0
    val schema = reader.getFooter.getFileMetaData.getSchema
    val columnIO = new ColumnIOFactory().getColumnIO(schema)
    var pages = reader.readNextRowGroup()
    while (pages != null) {
      val recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))
      for (_ <- 0 until pages.getRowCount.toInt) {
        val simpleGroup = recordReader.read.asInstanceOf[SimpleGroup]
        id = Math.max(id, simpleGroup.getInteger(column, 0))
      }
      pages = reader.readNextRowGroup()
    }
    reader.close()
    id
  }

  private def getRowsToRows: Row => Row = {
    val gmdc = this.groupMetadataCache
    //Converts the Cassandra rows to Spark rows and reconstruct start time as a long value
    //Schema: Int, java.sql.Timestamp, java.sql.Timestamp, Int, Int, Array[Byte], Array[Byte]
    row => {
      val gid = row.getDecimal(0).intValue()
      val gaps = row.getDecimal(2).longValue()
      val size: Long = row.getDecimal(5).longValue()
      val endTime = row.getTimestamp(1)
      val mid = row.getDecimal(3).intValue()
      val params = row.getAs[Array[Byte]](4)

      //Reconstructs the gaps array from the bit flag
      val gapsArray = Static.bitsToGaps(gaps.longValue(), gmdc(gid))

      //Retrieves the resolution from the metadata cache so the actual rows can be reconstructed
      val startTime = endTime.getTime - (size * gmdc(gid)(0))
      Row(gid, new Timestamp(startTime), endTime, mid, params, gapsArray)
    }
  }

  /**  Instance Variables **/
  private var sparkSession: SparkSession = _
  private var segment: DataFrame = _
}