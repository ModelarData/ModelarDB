package dk.aau.modelardb.engines.h2

import java.sql.DriverManager
import dk.aau.modelardb.Interface
import dk.aau.modelardb.core.utility.Static
import dk.aau.modelardb.core.{Configuration, Dimensions, Storage}
import dk.aau.modelardb.engines.RDBMSEngineUtilities
import dk.aau.modelardb.core.Dimensions.Types
import org.h2.expression.condition.Comparison
import org.h2.expression.{ExpressionColumn, ValueExpression}
import org.h2.table.TableFilter
import org.h2.value.ValueInt

class H2(configuration: Configuration, storage: Storage) {
  /** Public Methods **/
  def start(): Unit = {
    //Engine
    //http://www.h2database.com/html/features.html#in_memory_databases
    val connection = DriverManager.getConnection("jdbc:h2:mem:")
    val stmt = connection.createStatement()
    //https://www.h2database.com/html/commands.html#create_table
    stmt.execute(H2.getCreateDataPointViewSQL(configuration.getDimensions))
    stmt.execute(H2.getCreateSegmentViewSQL(configuration.getDimensions))
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
  /** Type Variables * */
  private val compareTypeField = classOf[Comparison].getDeclaredField("compareType")
  this.compareTypeField.setAccessible(true)
  private val compareTypeMethod = classOf[Comparison].getDeclaredMethod("getCompareOperator", classOf[Int])
  this.compareTypeMethod.setAccessible(true)

  /** Public Methods **/
  def getCreateDataPointViewSQL(dimensions: Dimensions): String = {
    s"""CREATE TABLE DataPoint(sid INT, timestamp TIMESTAMP, value REAL${getDimensionColumns(dimensions)})
       |ENGINE "dk.aau.modelardb.engines.h2.ViewDataPoint";
       |""".stripMargin
  }

  def getCreateSegmentViewSQL(dimensions: Dimensions): String = {
    s"""CREATE TABLE Segment
       |(sid INT, start_time TIMESTAMP, end_time TIMESTAMP, resolution INT, mid INT, parameters BYTEA, gaps BYTEA${getDimensionColumns(dimensions)})
       |ENGINE "dk.aau.modelardb.engines.h2.ViewSegment";
       |""".stripMargin
  }

  def tableFilterToSQLPredicates(filter: TableFilter, sgc: Array[Int]): String = {
    //TODO: determine if the optimize method actually does anything and if it should be called before parsing the predicates
    //TODO: decide if data point view => segment view predicates should be converted here (must be implemented for
    // each of the storage format) or in segment view like for Spark (slower as three queries are run instead of two)
    //TODO: Implement a proper recursive parser for H2's "AST" using a stack or @tailrec to ensure it is tail recursive
    //TODO: Determine if the comparison operator really cannot be extracted in a proper way
    filter.getSelect.getCondition() match {
      case null => ""
      case c: Comparison =>
        //HACK: Extracts the operator from the sub-tree using reflection as compareType seems to be completely inaccessible
        val operator = this.compareTypeMethod.invoke(c, this.compareTypeField.get(c)).asInstanceOf[String]
        val ec = c.getSubexpression(0).asInstanceOf[ExpressionColumn]
        val ve = c.getSubexpression(1).asInstanceOf[ValueExpression]
        val columnNameAndOperator = ec.getColumnName + " " + operator
        columnNameAndOperator match {
          case "SID =" => "GID = " + sgc(ve.getValue(null).asInstanceOf[ValueInt].getInt) //Session is ignored
          case p => Static.warn("ModelarDB: unsupported predicate " + p, 120); ""
        }
      case p => Static.warn("ModelarDB: unsupported predicate " + p, 120); ""
    }
  }

  /** Private Methods **/
  private def getDimensionColumns(dimensions: Dimensions): String = {
    if (dimensions.getColumns.isEmpty) {
      ""
    } else {
      dimensions.getColumns.zip(dimensions.getTypes).map {
        case (name, Types.INT) => name + " INT"
        case (name, Types.LONG) => name + " BIGINT"
        case (name, Types.FLOAT) => name + " REAL"
        case (name, Types.DOUBLE) => name + " DOUBLE"
        case (name, Types.TEXT) => name + " TEXT"
      }.mkString(", ", ", ", "")
    }
  }
}