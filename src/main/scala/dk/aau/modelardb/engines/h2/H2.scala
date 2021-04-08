package dk.aau.modelardb.engines.h2

import java.sql.DriverManager
import dk.aau.modelardb.Interface
import dk.aau.modelardb.core.utility.Static
import dk.aau.modelardb.core.{Configuration, Dimensions, Storage}
import dk.aau.modelardb.engines.{PredicatePushDown, RDBMSEngineUtilities}
import dk.aau.modelardb.core.Dimensions.Types
import org.h2.expression.condition.{Comparison, ConditionAndOr, ConditionInConstantSet}
import org.h2.expression.{Expression, ExpressionColumn, ValueExpression}
import org.h2.value.{ValueInt, ValueTimestamp}
import java.util.TimeZone
import java.util.HashMap

class H2(configuration: Configuration, storage: Storage) {
  /** Public Methods **/
  def start(): Unit = {
    //Initialize
    //Documentation: http://www.h2database.com/html/features.html#in_memory_databases
    val connection = DriverManager.getConnection("jdbc:h2:mem:")
    val stmt = connection.createStatement()
    //Documentation: https://www.h2database.com/html/commands.html#create_table
    stmt.execute(H2.getCreateDataPointViewSQL(configuration.getDimensions))
    stmt.execute(H2.getCreateSegmentViewSQL(configuration.getDimensions))
    //Documentation: https://www.h2database.com/html/commands.html#create_aggregate
    stmt.execute(H2.getCreateUDAFSQL("COUNT_S"))
    stmt.execute(H2.getCreateUDAFSQL("MIN_S"))
    stmt.execute(H2.getCreateUDAFSQL("MAX_S"))
    stmt.execute(H2.getCreateUDAFSQL("SUM_S"))
    stmt.execute(H2.getCreateUDAFSQL("AVG_S"))

    stmt.execute(H2.getCreateUDAFSQL("COUNT_MONTH"))
    stmt.execute(H2.getCreateUDAFSQL("MIN_MONTH"))
    stmt.execute(H2.getCreateUDAFSQL("MAX_MONTH"))
    stmt.execute(H2.getCreateUDAFSQL("SUM_MONTH"))
    stmt.execute(H2.getCreateUDAFSQL("AVG_MONTH"))
    stmt.close()

    //Ingestion
    RDBMSEngineUtilities.initialize(configuration, storage)
    val utilities = RDBMSEngineUtilities.getUtilities
    utilities.startIngestion()

    //Interface
    Interface.start(configuration, q => utilities.executeQuery(connection, q))

    //Shutdown
    connection.close()
    RDBMSEngineUtilities.waitUntilIngestionIsDone()
  }
}

object H2 {

  /** Instance Variables * */
  private val compareTypeField = classOf[Comparison].getDeclaredField("compareType")
  this.compareTypeField.setAccessible(true)
  private val compareTypeMethod = classOf[Comparison].getDeclaredMethod("getCompareOperator", classOf[Int])
  this.compareTypeMethod.setAccessible(true)
  private val andOrTypeField = classOf[ConditionAndOr].getDeclaredField("andOrType")
  this.andOrTypeField.setAccessible(true)

  /** Public Methods **/
  //Data Point View
  def getCreateDataPointViewSQL(dimensions: Dimensions): String = {
    s"""CREATE TABLE DataPoint(sid INT, timestamp TIMESTAMP, value REAL${H2.getDimensionColumns(dimensions)})
       |ENGINE "dk.aau.modelardb.engines.h2.ViewDataPoint";
       |""".stripMargin
  }

  //Segment View
  def getCreateSegmentViewSQL(dimensions: Dimensions): String = {
    s"""CREATE TABLE Segment
       |(sid INT, start_time TIMESTAMP, end_time TIMESTAMP, resolution INT, mid INT, parameters BYTEA, gaps BYTEA${H2.getDimensionColumns(dimensions)})
       |ENGINE "dk.aau.modelardb.engines.h2.ViewSegment";
       |""".stripMargin
  }

  //Segment View UDAFs
  def getCreateUDAFSQL(sqlName: String): String = {
    val splitSQLName = sqlName.split("_")
    val className = splitSQLName.map(_.toLowerCase.capitalize).mkString("")
    s"""CREATE AGGREGATE $sqlName FOR "dk.aau.modelardb.engines.h2.$className";"""
  }

  def expressionToSQLPredicates(expression: Expression, sgc: Array[Int], idc: HashMap[String, HashMap[Object, Array[Integer]]]): String = {
    expression match {
      //NO PREDICATES
      case null => ""
      //COLUMN OPERATOR VALUE
      case c: Comparison =>
        //HACK: Extracts the operator from the sub-tree using reflection as compareType seems to be completely inaccessible
        val operator = this.compareTypeMethod.invoke(c, this.compareTypeField.get(c)).asInstanceOf[String]
        val ec = c.getSubexpression(0).asInstanceOf[ExpressionColumn]
        val ve = c.getSubexpression(1).asInstanceOf[ValueExpression]
        (ec.getColumnName, operator) match {
          //SID
          case ("SID", "=") => val sid = ve.getValue(null).asInstanceOf[ValueInt].getInt
            " GID = " + PredicatePushDown.sidPointToGidPoint(sid, sgc)
          //TIMESTAMP
          case ("TIMESTAMP", ">") => " END_TIME > " + ve.getValue(null).asInstanceOf[ValueTimestamp].getTimestamp(TimeZone.getDefault).getTime
          case ("TIMESTAMP", ">=") => " END_TIME >= " + ve.getValue(null).asInstanceOf[ValueTimestamp].getTimestamp(TimeZone.getDefault).getTime
          case ("TIMESTAMP", "<") => " STAT_TIME < " + ve.getValue(null).asInstanceOf[ValueTimestamp].getTimestamp(TimeZone.getDefault).getTime
          case ("TIMESTAMP", "<=") => " START_TIME <= " + ve.getValue(null).asInstanceOf[ValueTimestamp].getTimestamp(TimeZone.getDefault).getTime
          case ("TIMESTAMP", "=") =>
            " (START_TIME <= " + ve.getValue(null).asInstanceOf[ValueTimestamp].getTimestamp(TimeZone.getDefault).getTime +
              " AND END_TIME >= " + ve.getValue(null).asInstanceOf[ValueTimestamp].getTimestamp(TimeZone.getDefault).getTime + ")"
          //DIMENSIONS
          case (columnName, "=") if idc.containsKey(columnName) =>
            PredicatePushDown.dimensionEqualToGidIn(columnName, ve.getValue(null).getObject(), idc).mkString("GID IN (", ",", ")")
          case p => Static.warn("ModelarDB: unsupported predicate " + p, 120); ""
        }
      //IN
      case cin: ConditionInConstantSet =>
        cin.getSubexpression(0).getColumnName match {
          case "SID" =>
            val sids = Array.fill[Any](cin.getSubexpressionCount - 1)(0) //The first value is the column name
            for (i <- Range(1, cin.getSubexpressionCount)) {
              sids(i - 1) = cin.getSubexpression(i).getValue(null).asInstanceOf[ValueInt].getInt
            }
            PredicatePushDown.sidInToGidIn(sids, sgc).mkString("GID IN (", ",", ")")
          case p => Static.warn("ModelarDB: unsupported predicate " + p, 120); ""
        }
      //AND
      case cao: ConditionAndOr if this.andOrTypeField.getInt(cao) == ConditionAndOr.AND => "(" +
        expressionToSQLPredicates(cao.getSubexpression(0), sgc, idc) + " AND " +
        expressionToSQLPredicates(cao.getSubexpression(1), sgc, idc) + ")"
      //OR
      case cao: ConditionAndOr if this.andOrTypeField.getInt(cao) == ConditionAndOr.OR => "(" +
        expressionToSQLPredicates(cao.getSubexpression(0), sgc, idc) + " OR " +
        expressionToSQLPredicates(cao.getSubexpression(1), sgc, idc) + ")"
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
        case (name, Types.TEXT) => name + " VARCHAR"
      }.mkString(", ", ", ", "")
    }
  }
}