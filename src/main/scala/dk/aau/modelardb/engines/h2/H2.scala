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
    Interface.start(configuration, q => utilities.executeQuery(connection, q))

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
    //TODO: determine if the optimize method actually does anything and if it should be called before parsing the predicates
    //TODO: decide if data point view => segment view predicates should be converted here (must be implemented for
    // each of the storage format) or in segment view like for Spark (slower as three queries are run instead of two)
    //TODO: Implement a proper recursive parser for H2's "AST" using a stack or @tailrec
    filter.getSelect.getCondition() match {
      case null => ""
      //https://github.com/h2database/h2database/blob/master/h2/src/main/org/h2/expression/condition/Comparison.java
      case c: Comparison => c.getSubexpression(0) match {
        //TODO: Translate time series id to time series group id like in Segment View
        //https://github.com/h2database/h2database/blob/master/h2/src/main/org/h2/expression/ExpressionColumn.java
        case ec: ExpressionColumn if ec.getColumnName == "SID" =>
          //https://github.com/h2database/h2database/blob/master/h2/src/main/org/h2/expression/ValueExpression.java
          //TODO: Determine if we can get compareType from Comparison of if we to use getSQL and than remove the left branch
          "gid = " + c.getSubexpression(1).asInstanceOf[ValueExpression].getValue(null) //Session is ignored
        case p => Static.warn("ModelarDB: unsupported predicate for H2 " + p, 120); ""
      }
      case p => Static.warn("ModelarDB: unsupported predicate for H2 " + p, 120); ""
    }
  }
}