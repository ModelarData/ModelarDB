package dk.aau.modelardb.engines.hsqldb

import dk.aau.modelardb.engines.RDBMSEngineUtilities
import org.hsqldb.Row
import org.hsqldb.jdbc.JDBCResultSet
import org.hsqldb.navigator.RowSetNavigator
import org.hsqldb.result.{Result, ResultMetaData}
import org.hsqldb.rowio.{RowInputInterface, RowOutputInterface}
import org.hsqldb.types.{TimestampData, Type}

import scala.collection.JavaConverters.asScalaIteratorConverter

//http://hsqldb.org/doc/2.0/guide/sqlroutines-chapt.html#src_psm_return_statement
//http://hsqldb.org/doc/2.0/guide/sqlroutines-chapt.html#src_jrt_routines
object ViewDataPoint {
  //The JDBCConnection used can be specified as the first argument like this (connection: Connection)
  def queryView: JDBCResultSet = {
    val metadata = ResultMetaData.newResultMetaData(ViewDataPoint.types, ViewDataPoint.labels)
    val result = Result.newDataResult(metadata)
    result.setNavigator(new ViewDataPoint)
    JDBCResultSet.newJDBCResultSet(result, metadata)
  }

  /** Instance Variables **/
  private val labels: Array[String] = Array("sid", "timestamp", "value")
  private val types: Array[Type] = Array(Type.SQL_INTEGER, Type.SQL_TIMESTAMP, Type.SQL_DOUBLE)
}


class ViewDataPoint extends RowSetNavigator {
  override def next: Boolean = {
    if (this.dataPoints.hasNext) {
      //The data points are different in the result despite modifying the array
      val dataPoint = this.dataPoints.next()
      this.currentRow(0) = dataPoint.sid.asInstanceOf[AnyRef]
      this.currentRow(1) = new TimestampData(dataPoint.timestamp / 1000).asInstanceOf[AnyRef]
      this.currentRow(2) = dataPoint.value.asInstanceOf[AnyRef]
      true
    } else {
      false
    }
  }

  override def getCurrent: Array[AnyRef] = this.currentRow

  override def getCurrentRow: Row = ???

  override def add(data: Array[AnyRef]): Unit = ???

  override def addRow(row: Row): Boolean = ???

  override def removeCurrent(): Unit = ???

  override def clear(): Unit = ???

  override def release(): Unit = ???

  override def write(out: RowOutputInterface, meta: ResultMetaData): Unit = ???

  override def read(in: RowInputInterface, meta: ResultMetaData): Unit = ???

  /** Instance Variables **/
  val storage = RDBMSEngineUtilities.getStorage
  val dataPoints = (storage.asInstanceOf[HSQLDBStorage].getSegmentGroups() ++
    RDBMSEngineUtilities.getUtilities.getInMemorySegmentGroups())
    .flatMap(sg => sg.toSegments(storage))
    .flatMap(segment => segment.grid().iterator().asScala)
  var currentRow: Array[AnyRef] = new Array[AnyRef](3)
}