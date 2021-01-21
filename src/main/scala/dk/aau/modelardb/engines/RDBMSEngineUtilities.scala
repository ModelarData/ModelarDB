package dk.aau.modelardb.engines

import java.sql.Connection
import scala.collection.mutable.ArrayBuffer

object RDBMSEngineUtilities {

  /** Public methods **/
  def sql(connection: Connection, query: String): Array[String] = {
    //Execute Query
    val stmt = connection.createStatement()
    stmt.execute(query)
    val rs = stmt.getResultSet
    val md = rs.getMetaData

    //Format Result
    val result = ArrayBuffer[String]()
    val line = new StringBuilder()
    val columnSeparators = md.getColumnCount
    while (rs.next()) {
      var columnIndex = 1
      line.append('{')
      while (columnIndex < columnSeparators) {
        addColumn(md.getColumnName(columnIndex), rs.getObject(columnIndex), ',', line)
        columnIndex += 1
      }
      addColumn(md.getColumnName(columnIndex), rs.getObject(columnIndex), '}', line)
      result.append(line.mkString)
      line.clear()
    }

    //Close and Return
    rs.close()
    stmt.close()
    result.toArray
  }

  /** Private Methods **/
  private def addColumn(columnName: String, value: AnyRef, end: Char, output: StringBuilder): Unit = {
    output.append('"')
    output.append(columnName)
    output.append('"')
    output.append(':')

    //Numbers should not be quoted
    if (value.isInstanceOf[Int] || value.isInstanceOf[Float]) {
      output.append(value)
    } else {
      output.append('"')
      output.append(value)
      output.append('"')
    }
    output.append(end)
  }
}