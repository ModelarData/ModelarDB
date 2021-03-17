package dk.aau.modelardb.engines.derby

import dk.aau.modelardb.core.models
import dk.aau.modelardb.engines.RDBMSEngineUtilities
import org.apache.derby.vti.{RestrictedVTI, Restriction}

import java.io.{InputStream, Reader}
import java.net.URL
import java.sql.{Blob, Clob, Date, NClob, Ref, ResultSetMetaData, RowId, SQLException, SQLWarning, SQLXML, Statement, Time, Timestamp}
import java.util.Calendar
import java.{sql, util}

object SegmentView {
  def apply: SegmentView = new SegmentView()
}

//noinspection NotImplementedCode
class SegmentView extends java.sql.ResultSet with RestrictedVTI {

  // 1-based as this matches ResultSet
  private val index2columns = Map(
    1 -> "sid",
    2 -> "start_time",
    3 -> "end_time",
    4 -> "resolution"
  )

  private val maxIndex = index2columns.keys.max

  private def getLabel(columnIndex: Int): String =
    index2columns.getOrElse(columnIndex,
      throw new SQLException(s"Column index out of range. Max is $maxIndex"))


  private var segments: Iterator[models.Segment] = null
  private var segment: models.Segment = null

  override def initScan(columns: Array[String], restriction: Restriction): Unit = {
    val storage = RDBMSEngineUtilities.getStorage match {
      case derby: DerbyStorage => derby
      case _ => throw new Exception("error")
    }
    segments = storage
      .getSegmentGroups(restriction)
      .flatMap(_.toSegments(RDBMSEngineUtilities.getStorage))
  }


  /* Implemented ResultSet methods */

  override def next(): Boolean = {
    if (segments.hasNext) {
      segment = segments.next()
      true
    } else { false }
  }

  override def close(): Unit = {}

  override def wasNull(): Boolean = false

  override def getWarnings: SQLWarning = null //HACK: nothing can go wrong....

  override def getInt(columnIndex: Int): Int = {
    val columnLabel = getLabel(columnIndex)
    getInt(columnLabel)
  }

  override def getInt(columnLabel: String): Int = {
    columnLabel match {
      case "sid" => segment.sid
      case "resolution" => segment.resolution
      case _ =>  throw new SQLException(s"Column label $columnLabel does not contain a Int")
    }
  }

  override def getTimestamp(columnIndex: Int): Timestamp = {
    val columnLabel = getLabel(columnIndex)
    getTimestamp(columnLabel)
  }

  override def getTimestamp(columnLabel: String): Timestamp = {
    columnLabel match {
      case "start_time" => new Timestamp(segment.getStartTime)
      case "end_time" => new Timestamp(segment.getEndTime)
      case _ => throw new SQLException(s"Column label $columnLabel does not contain a Long")
    }
  }


  /* Unimplemented methods */

  override def getLong(columnIndex: Int): Long = ???

  override def getLong(columnLabel: String): Long = ???

  override def getString(columnIndex: Int): String = ???

  override def getBoolean(columnIndex: Int): Boolean = ???

  override def getByte(columnIndex: Int): Byte = ???

  override def getShort(columnIndex: Int): Short = ???

  override def getFloat(columnIndex: Int): Float = ???

  override def getDouble(columnIndex: Int): Double = ???

  override def getBigDecimal(columnIndex: Int, scale: Int): java.math.BigDecimal = ???

  override def getBytes(columnIndex: Int): Array[Byte] = ???

  override def getDate(columnIndex: Int): Date = ???

  override def getTime(columnIndex: Int): Time = ???

  override def getAsciiStream(columnIndex: Int): InputStream = ???

  override def getUnicodeStream(columnIndex: Int): InputStream = ???

  override def getBinaryStream(columnIndex: Int): InputStream = ???

  override def getString(columnLabel: String): String = ???

  override def getBoolean(columnLabel: String): Boolean = ???

  override def getByte(columnLabel: String): Byte = ???

  override def getShort(columnLabel: String): Short = ???

  override def getFloat(columnLabel: String): Float = ???

  override def getDouble(columnLabel: String): Double = ???

  override def getBigDecimal(columnLabel: String, scale: Int): java.math.BigDecimal = ???

  override def getBytes(columnLabel: String): Array[Byte] = ???

  override def getDate(columnLabel: String): Date = ???

  override def getTime(columnLabel: String): Time = ???

  override def getAsciiStream(columnLabel: String): InputStream = ???

  override def getUnicodeStream(columnLabel: String): InputStream = ???

  override def getBinaryStream(columnLabel: String): InputStream = ???

  override def clearWarnings(): Unit = ???

  override def getCursorName: String = ???

  override def getMetaData: ResultSetMetaData = ???

  override def getObject(columnIndex: Int): AnyRef = ???

  override def getObject(columnLabel: String): AnyRef = ???

  override def findColumn(columnLabel: String): Int = ???

  override def getCharacterStream(columnIndex: Int): Reader = ???

  override def getCharacterStream(columnLabel: String): Reader = ???

  override def getBigDecimal(columnIndex: Int): java.math.BigDecimal = ???

  override def getBigDecimal(columnLabel: String): java.math.BigDecimal = ???

  override def isBeforeFirst: Boolean = ???

  override def isAfterLast: Boolean = ???

  override def isFirst: Boolean = ???

  override def isLast: Boolean = ???

  override def beforeFirst(): Unit = ???

  override def afterLast(): Unit = ???

  override def first(): Boolean = ???

  override def last(): Boolean = ???

  override def getRow: Int = ???

  override def absolute(row: Int): Boolean = ???

  override def relative(rows: Int): Boolean = ???

  override def previous(): Boolean = ???

  override def setFetchDirection(direction: Int): Unit = ???

  override def getFetchDirection: Int = ???

  override def setFetchSize(rows: Int): Unit = ???

  override def getFetchSize: Int = ???

  override def getType: Int = ???

  override def getConcurrency: Int = ???

  override def rowUpdated(): Boolean = ???

  override def rowInserted(): Boolean = ???

  override def rowDeleted(): Boolean = ???

  override def updateNull(columnIndex: Int): Unit = ???

  override def updateBoolean(columnIndex: Int, x: Boolean): Unit = ???

  override def updateByte(columnIndex: Int, x: Byte): Unit = ???

  override def updateShort(columnIndex: Int, x: Short): Unit = ???

  override def updateInt(columnIndex: Int, x: Int): Unit = ???

  override def updateLong(columnIndex: Int, x: Long): Unit = ???

  override def updateFloat(columnIndex: Int, x: Float): Unit = ???

  override def updateDouble(columnIndex: Int, x: Double): Unit = ???

  override def updateBigDecimal(columnIndex: Int, x: java.math.BigDecimal): Unit = ???

  override def updateString(columnIndex: Int, x: String): Unit = ???

  override def updateBytes(columnIndex: Int, x: Array[Byte]): Unit = ???

  override def updateDate(columnIndex: Int, x: Date): Unit = ???

  override def updateTime(columnIndex: Int, x: Time): Unit = ???

  override def updateTimestamp(columnIndex: Int, x: Timestamp): Unit = ???

  override def updateAsciiStream(columnIndex: Int, x: InputStream, length: Int): Unit = ???

  override def updateBinaryStream(columnIndex: Int, x: InputStream, length: Int): Unit = ???

  override def updateCharacterStream(columnIndex: Int, x: Reader, length: Int): Unit = ???

  override def updateObject(columnIndex: Int, x: Any, scaleOrLength: Int): Unit = ???

  override def updateObject(columnIndex: Int, x: Any): Unit = ???

  override def updateNull(columnLabel: String): Unit = ???

  override def updateBoolean(columnLabel: String, x: Boolean): Unit = ???

  override def updateByte(columnLabel: String, x: Byte): Unit = ???

  override def updateShort(columnLabel: String, x: Short): Unit = ???

  override def updateInt(columnLabel: String, x: Int): Unit = ???

  override def updateLong(columnLabel: String, x: Long): Unit = ???

  override def updateFloat(columnLabel: String, x: Float): Unit = ???

  override def updateDouble(columnLabel: String, x: Double): Unit = ???

  override def updateBigDecimal(columnLabel: String, x: java.math.BigDecimal): Unit = ???

  override def updateString(columnLabel: String, x: String): Unit = ???

  override def updateBytes(columnLabel: String, x: Array[Byte]): Unit = ???

  override def updateDate(columnLabel: String, x: Date): Unit = ???

  override def updateTime(columnLabel: String, x: Time): Unit = ???

  override def updateTimestamp(columnLabel: String, x: Timestamp): Unit = ???

  override def updateAsciiStream(columnLabel: String, x: InputStream, length: Int): Unit = ???

  override def updateBinaryStream(columnLabel: String, x: InputStream, length: Int): Unit = ???

  override def updateCharacterStream(columnLabel: String, reader: Reader, length: Int): Unit = ???

  override def updateObject(columnLabel: String, x: Any, scaleOrLength: Int): Unit = ???

  override def updateObject(columnLabel: String, x: Any): Unit = ???

  override def insertRow(): Unit = ???

  override def updateRow(): Unit = ???

  override def deleteRow(): Unit = ???

  override def refreshRow(): Unit = ???

  override def cancelRowUpdates(): Unit = ???

  override def moveToInsertRow(): Unit = ???

  override def moveToCurrentRow(): Unit = ???

  override def getStatement: Statement = ???

  override def getObject(columnIndex: Int, map: util.Map[String, Class[_]]): AnyRef = ???

  override def getRef(columnIndex: Int): Ref = ???

  override def getBlob(columnIndex: Int): Blob = ???

  override def getClob(columnIndex: Int): Clob = ???

  override def getArray(columnIndex: Int): sql.Array = ???

  override def getObject(columnLabel: String, map: util.Map[String, Class[_]]): AnyRef = ???

  override def getRef(columnLabel: String): Ref = ???

  override def getBlob(columnLabel: String): Blob = ???

  override def getClob(columnLabel: String): Clob = ???

  override def getArray(columnLabel: String): sql.Array = ???

  override def getDate(columnIndex: Int, cal: Calendar): Date = ???

  override def getDate(columnLabel: String, cal: Calendar): Date = ???

  override def getTime(columnIndex: Int, cal: Calendar): Time = ???

  override def getTime(columnLabel: String, cal: Calendar): Time = ???

  override def getTimestamp(columnIndex: Int, cal: Calendar): Timestamp = ???

  override def getTimestamp(columnLabel: String, cal: Calendar): Timestamp = ???

  override def getURL(columnIndex: Int): URL = ???

  override def getURL(columnLabel: String): URL = ???

  override def updateRef(columnIndex: Int, x: Ref): Unit = ???

  override def updateRef(columnLabel: String, x: Ref): Unit = ???

  override def updateBlob(columnIndex: Int, x: Blob): Unit = ???

  override def updateBlob(columnLabel: String, x: Blob): Unit = ???

  override def updateClob(columnIndex: Int, x: Clob): Unit = ???

  override def updateClob(columnLabel: String, x: Clob): Unit = ???

  override def updateArray(columnIndex: Int, x: sql.Array): Unit = ???

  override def updateArray(columnLabel: String, x: sql.Array): Unit = ???

  override def getRowId(columnIndex: Int): RowId = ???

  override def getRowId(columnLabel: String): RowId = ???

  override def updateRowId(columnIndex: Int, x: RowId): Unit = ???

  override def updateRowId(columnLabel: String, x: RowId): Unit = ???

  override def getHoldability: Int = ???

  override def isClosed: Boolean = ???

  override def updateNString(columnIndex: Int, nString: String): Unit = ???

  override def updateNString(columnLabel: String, nString: String): Unit = ???

  override def updateNClob(columnIndex: Int, nClob: NClob): Unit = ???

  override def updateNClob(columnLabel: String, nClob: NClob): Unit = ???

  override def getNClob(columnIndex: Int): NClob = ???

  override def getNClob(columnLabel: String): NClob = ???

  override def getSQLXML(columnIndex: Int): SQLXML = ???

  override def getSQLXML(columnLabel: String): SQLXML = ???

  override def updateSQLXML(columnIndex: Int, xmlObject: SQLXML): Unit = ???

  override def updateSQLXML(columnLabel: String, xmlObject: SQLXML): Unit = ???

  override def getNString(columnIndex: Int): String = ???

  override def getNString(columnLabel: String): String = ???

  override def getNCharacterStream(columnIndex: Int): Reader = ???

  override def getNCharacterStream(columnLabel: String): Reader = ???

  override def updateNCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit = ???

  override def updateNCharacterStream(columnLabel: String, reader: Reader, length: Long): Unit = ???

  override def updateAsciiStream(columnIndex: Int, x: InputStream, length: Long): Unit = ???

  override def updateBinaryStream(columnIndex: Int, x: InputStream, length: Long): Unit = ???

  override def updateCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit = ???

  override def updateAsciiStream(columnLabel: String, x: InputStream, length: Long): Unit = ???

  override def updateBinaryStream(columnLabel: String, x: InputStream, length: Long): Unit = ???

  override def updateCharacterStream(columnLabel: String, reader: Reader, length: Long): Unit = ???

  override def updateBlob(columnIndex: Int, inputStream: InputStream, length: Long): Unit = ???

  override def updateBlob(columnLabel: String, inputStream: InputStream, length: Long): Unit = ???

  override def updateClob(columnIndex: Int, reader: Reader, length: Long): Unit = ???

  override def updateClob(columnLabel: String, reader: Reader, length: Long): Unit = ???

  override def updateNClob(columnIndex: Int, reader: Reader, length: Long): Unit = ???

  override def updateNClob(columnLabel: String, reader: Reader, length: Long): Unit = ???

  override def updateNCharacterStream(columnIndex: Int, x: Reader): Unit = ???

  override def updateNCharacterStream(columnLabel: String, reader: Reader): Unit = ???

  override def updateAsciiStream(columnIndex: Int, x: InputStream): Unit = ???

  override def updateBinaryStream(columnIndex: Int, x: InputStream): Unit = ???

  override def updateCharacterStream(columnIndex: Int, x: Reader): Unit = ???

  override def updateAsciiStream(columnLabel: String, x: InputStream): Unit = ???

  override def updateBinaryStream(columnLabel: String, x: InputStream): Unit = ???

  override def updateCharacterStream(columnLabel: String, reader: Reader): Unit = ???

  override def updateBlob(columnIndex: Int, inputStream: InputStream): Unit = ???

  override def updateBlob(columnLabel: String, inputStream: InputStream): Unit = ???

  override def updateClob(columnIndex: Int, reader: Reader): Unit = ???

  override def updateClob(columnLabel: String, reader: Reader): Unit = ???

  override def updateNClob(columnIndex: Int, reader: Reader): Unit = ???

  override def updateNClob(columnLabel: String, reader: Reader): Unit = ???

  override def getObject[T](columnIndex: Int, `type`: Class[T]): T = ???

  override def getObject[T](columnLabel: String, `type`: Class[T]): T = ???

  override def unwrap[T](iface: Class[T]): T = ???

  override def isWrapperFor(iface: Class[_]): Boolean = ???
}
