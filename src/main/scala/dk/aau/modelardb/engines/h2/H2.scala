package dk.aau.modelardb.engines.h2

import java.sql.DriverManager
import dk.aau.modelardb.Interface
import dk.aau.modelardb.core.utility.Static
import dk.aau.modelardb.core.{Configuration, Storage}
import dk.aau.modelardb.engines.RDBMSEngineUtilities
import org.h2.expression.condition.Comparison
import org.h2.expression.{ExpressionColumn, ValueExpression}
import org.h2.table.TableFilter

class H2(configuration: Configuration, storage: Storage) {
  /** Public Methods **/
  def start(): Unit = {
    //Engine
    //http://www.h2database.com/html/features.html#in_memory_databases
    val connection = DriverManager.getConnection("jdbc:h2:mem:")
    val stmt = connection.createStatement()
    //https://www.h2database.com/html/commands.html#create_table
    //TODO: extend the schema of both views with the columns of the user-defined dimensions at run-time
    stmt.execute(H2.CreateDataPointViewSQL)
    stmt.execute(H2.CreateSegmentViewSQL)
    //https://www.h2database.com/html/commands.html#create_aggregate
    stmt.execute("CREATE AGGREGATE COUNT_S FOR \"dk.aau.modelardb.engines.h2.CountS\";")
    stmt.close()

    //Ingestion
    RDBMSEngineUtilities.initialize(configuration, storage)
    val utilities = RDBMSEngineUtilities.getUtilities
    utilities.startIngestion()

    //Interface
    Interface.start(
      configuration.getString("modelardb.interface"),
      q => utilities.executeQuery(connection, q)
    )

    //Shutdown
    connection.close()
  }
}

object H2 {
  /** Type Variables **/
  val CreateDataPointViewSQL = """CREATE TABLE DataPoint(sid INT, timestamp TIMESTAMP, value REAL)
                                 |ENGINE "dk.aau.modelardb.engines.h2.ViewDataPoint";
                                 |""".stripMargin
  val CreateSegmentViewSQL = """CREATE TABLE Segment
                               |(sid INT, start_time TIMESTAMP, end_time TIMESTAMP, resolution INT, mid INT, parameters BYTEA, gaps BYTEA)
                               |ENGINE "dk.aau.modelardb.engines.h2.ViewSegment";
                               |""".stripMargin

  /** Public Methods **/
  def tableFilterToSQLPredicates(filter: TableFilter): String = {
    //TODO: Implement proper recursive parser of H2's ast using a stack or tailrec
    val clause = filter.getSelect.getCondition() match {
      //https://github.com/h2database/h2database/blob/master/h2/src/main/org/h2/expression/condition/Comparison.java
      case c: Comparison => c.getSubexpression(0) match {
        //TODO: Translate time series id to time series group id
        //https://github.com/h2database/h2database/blob/master/h2/src/main/org/h2/expression/ExpressionColumn.java
        case ec: ExpressionColumn if ec.getColumnName == "SID" =>
          //https://github.com/h2database/h2database/blob/master/h2/src/main/org/h2/expression/ValueExpression.java
          "gid = " + c.getSubexpression(1).asInstanceOf[ValueExpression].getValue(null) //Session is ignored
        case p => Static.warn("ModelarDB: unsupported predicate for H2 " + p, 120); ""
      }
      case p => Static.warn("ModelarDB: unsupported predicate for H2 " + p, 120); ""
    }
    clause
  }
}