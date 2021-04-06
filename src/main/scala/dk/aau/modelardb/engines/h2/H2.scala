package dk.aau.modelardb.engines.h2

import java.sql.DriverManager
import dk.aau.modelardb.Interface
import dk.aau.modelardb.core.utility.Static
import dk.aau.modelardb.core.{Configuration, Dimensions, Storage}
import dk.aau.modelardb.engines.{PredicatePushDown, RDBMSEngineUtilities}
import dk.aau.modelardb.core.Dimensions.Types
import org.h2.expression.condition.{Comparison, ConditionInConstantSet}
import org.h2.expression.{ExpressionColumn, ValueExpression}
import org.h2.table.TableFilter
import org.h2.value.ValueInt

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
    stmt.close()
    H2.h2storage = storage.asInstanceOf[H2Storage]

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
  /** Type Variables * */
  private var h2storage: H2Storage = _
  private val compareTypeField = classOf[Comparison].getDeclaredField("compareType")
  this.compareTypeField.setAccessible(true)
  private val compareTypeMethod = classOf[Comparison].getDeclaredMethod("getCompareOperator", classOf[Int])
  this.compareTypeMethod.setAccessible(true)

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

  //Segment View UDAfs
  def getCreateUDAFSQL(sqlName: String): String = {
    val splitSQLName = sqlName.split("_")
    val className = splitSQLName.map(_.toLowerCase.capitalize).mkString("")
    s"""CREATE AGGREGATE $sqlName FOR "dk.aau.modelardb.engines.h2.$className";"""
  }

  def getH2Storage(): H2Storage = {
    this.h2storage
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
          //SID
          case "SID =" => val sid = ve.getValue(null).asInstanceOf[ValueInt].getInt
            s" GID = " + PredicatePushDown.sidPointToGidPoint(sid, sgc)
          case p => Static.warn("ModelarDB: unsupported predicate " + p, 120); ""
        }
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