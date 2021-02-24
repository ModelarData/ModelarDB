package dk.aau.modelardb.engines.hsqldb

import dk.aau.modelardb.engines.RDBMSEngineUtilities
import org.hsqldb.jdbc.{JDBCConnection, JDBCResultSet}
import org.hsqldb.result.{Result, ResultMetaData}
import org.hsqldb.types.{TimestampData, Type}

import java.sql.{Connection, ResultSet}

object ViewDataPoint {
  def queryViewNative(connection: Connection): ResultSet = {
    //TODO: How can we return multiple values from a JDBCReultSet without creating a new ResultSet?
    //val gdp = RDBMSEngineUtilities.getUtilities.getDataPoints()
    val metadata = ResultMetaData.newResultMetaData(ViewDataPoint.types, ViewDataPoint.labels)
    val result = Result.newDataResult(metadata)
    result.addBatchedPreparedExecuteRequest(Array(1.asInstanceOf[AnyRef],
      new TimestampData(System.currentTimeMillis() / 1000).asInstanceOf[AnyRef], 12.0F.asInstanceOf[AnyRef]))
    JDBCResultSet.newJDBCResultSet(result, metadata)
  }

  def queryView(connection: Connection): ResultSet = {
    //TODO: How can we return a ResultSet without hitting the error in org.hsqldb.Routine line 962-969?
    new dk.aau.modelardb.engines.derby.ViewDataPoint()
    queryViewNative(connection)
  }

  /** Instance Variables **/
  private val labels: Array[String] = Array("sid", "timestamp", "value")
  private val types: Array[Type] = Array(Type.SQL_INTEGER, Type.SQL_TIMESTAMP, Type.SQL_DOUBLE)
}