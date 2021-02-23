package dk.aau.modelardb.engines.hsqldb

import org.hsqldb.jdbc.JDBCResultSet
import org.hsqldb.result.{Result, ResultMetaData}

import java.sql.{Connection, ResultSet}

object ViewDataPoint {
  def queryView(connection: Connection): ResultSet = {
    val result = Result.emptyGeneratedResult
    val metadata = ResultMetaData.emptyResultMetaData
    JDBCResultSet.newJDBCResultSet(result, metadata)
  }
}
