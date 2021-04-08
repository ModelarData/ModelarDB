package dk.aau.modelardb.engines.derby

import java.sql.DriverManager
import dk.aau.modelardb.Interface
import dk.aau.modelardb.core.Dimensions.Types
import dk.aau.modelardb.core.utility.Static
import dk.aau.modelardb.core.{Configuration, Dimensions, Storage}
import dk.aau.modelardb.engines.{PredicatePushDown, RDBMSEngineUtilities}
import dk.aau.modelardb.engines.derby.Derby._
import org.apache.derby.vti.Restriction
import org.apache.derby.vti.Restriction.ColumnQualifier._
import java.util.HashMap

class Derby(configuration: Configuration, storage: Storage) {
  /** Public Methods **/
  def start(): Unit = {
    //Initialize
    /* Documentation:
     * https://db.apache.org/derby/docs/10.15/security/rsecpolicysample.html
     * https://db.apache.org/derby/docs/10.15/devguide/cdevdvlpinmemdb.html */
    val connection = DriverManager.getConnection("jdbc:derby:memory:;create=true")
    val stmt = connection.createStatement()

    /* Documentation:
     * https://db.apache.org/derby/docs/10.15/ref/rrefcreatefunctionstatement.html
     * https://db.apache.org/derby/docs/10.15/ref/rrefsqljexternalname.html
     * https://db.apache.org/derby/docs/10.15/ref/rrefsqlj15446.html */
    stmt.execute(Derby.getCreateDataPointFunctionSQL(configuration.getDimensions))
    stmt.execute(Derby.CreateDataPointViewSQL)
    stmt.execute(Derby.getCreateSegmentFunctionSQL(configuration.getDimensions))
    stmt.execute(Derby.CreateSegmentViewSQL)
    stmt.execute(Derby.CreateCountBigUDAFSQL)

    //Documentation: https://db.apache.org/derby/docs/10.15/ref/crefsqlj31068.html
    stmt.execute(CreateMapTypeSQL) //Apache Derby does not provide a map type

    /* Documentation:
     * https://db.apache.org/derby/docs/10.15/ref/rrefsqljcreatetype.html
     * https://db.apache.org/derby/docs/10.15/ref/rrefsqljexternalname.html */
    stmt.execute(CreateSegmentTypeSQL) //Apache Derby's UDAFs can only have one parameter

    /* Documentation:
     * https://db.apache.org/derby/docs/10.15/ref/rrefcreatefunctionstatement.html
     * https://db.apache.org/derby/docs/10.15/ref/rrefsqljexternalname.html */
    stmt.execute(CreateToSegmentFunctionSQL) //Apache Derby does not cast values to UDTs

    /* Documentation:
     * https://db.apache.org/derby/docs/10.15/ref/rrefsqljcreateaggregate.html
     * https://db.apache.org/derby/docs/10.15/ref/rrefsqljexternalname.html */
    stmt.execute(Derby.getCreateUDAFSQL("COUNT_S", "bigint"))
    stmt.execute(Derby.getCreateUDAFSQL("MIN_S", "real"))
    stmt.execute(Derby.getCreateUDAFSQL("MAX_S", "real"))
    stmt.execute(Derby.getCreateUDAFSQL("SUM_S", "real"))
    stmt.execute(Derby.getCreateUDAFSQL("AVG_S", "real"))

    stmt.execute(Derby.getCreateUDAFSQL("COUNT_MONTH", "map"))
    stmt.execute(Derby.getCreateUDAFSQL("MIN_MONTH", "map"))
    stmt.execute(Derby.getCreateUDAFSQL("MAX_MONTH", "map"))
    stmt.execute(Derby.getCreateUDAFSQL("SUM_MONTH", "map"))
    stmt.execute(Derby.getCreateUDAFSQL("AVG_MONTH", "map"))
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

object Derby {

  /** Public Methods and Instance Variables **/
  //Data Point View
  def getCreateDataPointFunctionSQL(dimensions: Dimensions): String = {
    s"""CREATE FUNCTION DataPoint()
       |RETURNS TABLE (sid INT, timestamp TIMESTAMP, value REAL${Derby.getDimensionColumns(dimensions)})
       |LANGUAGE JAVA
       |PARAMETER STYLE DERBY_JDBC_RESULT_SET
       |NO SQL
       |EXTERNAL NAME 'dk.aau.modelardb.engines.derby.ViewDataPoint.apply'
       |""".stripMargin
  }

  val CreateDataPointViewSQL = "CREATE VIEW DataPoint as SELECT d.* FROM TABLE(DataPoint()) d"

  val CreateCountBigUDAFSQL = "CREATE DERBY AGGREGATE count_big FOR int RETURNS bigint EXTERNAL NAME 'dk.aau.modelardb.engines.derby.CountBig'"

  //Segment View
  def getCreateSegmentFunctionSQL(dimensions: Dimensions): String = {
    s"""CREATE FUNCTION Segment()
       |RETURNS TABLE (sid INT, start_time TIMESTAMP, end_time TIMESTAMP, resolution INT, mid INT, parameters LONG VARCHAR FOR BIT DATA, gaps LONG VARCHAR FOR BIT DATA${Derby.getDimensionColumns(dimensions)})
       |LANGUAGE JAVA
       |PARAMETER STYLE DERBY_JDBC_RESULT_SET
       |NO SQL
       |EXTERNAL NAME 'dk.aau.modelardb.engines.derby.ViewSegment.apply'
       |""".stripMargin
  }

  val CreateSegmentViewSQL = "CREATE VIEW Segment AS SELECT s.* FROM TABLE(Segment()) s"

  //Segment View UDAFs
  val CreateSegmentTypeSQL: String =
    """CREATE TYPE segment
      |EXTERNAL NAME 'dk.aau.modelardb.engines.derby.SegmentData'
      |LANGUAGE JAVA
      |""".stripMargin

  val CreateToSegmentFunctionSQL: String =
    """CREATE FUNCTION TO_SEGMENT(sid INT, start_time TIMESTAMP, end_time TIMESTAMP, resolution INT, mid INT, parameters LONG VARCHAR FOR BIT DATA, gaps LONG VARCHAR FOR BIT DATA)
      |RETURNS segment
      |LANGUAGE JAVA
      |PARAMETER STYLE JAVA
      |NO SQL
      |EXTERNAL NAME 'dk.aau.modelardb.engines.derby.SegmentData.apply'""".stripMargin

  val CreateMapTypeSQL: String =
    """CREATE TYPE map
      |EXTERNAL NAME 'dk.aau.modelardb.engines.derby.DerbyMap'
      |LANGUAGE JAVA
      |""".stripMargin

  def getCreateUDAFSQL(sqlName: String, returnType: String): String = {
    val splitSQLName = sqlName.split("_")
    val className = splitSQLName.map(_.toLowerCase.capitalize).mkString("")
    s"""CREATE DERBY AGGREGATE $sqlName FOR segment
       |RETURNS $returnType
       |EXTERNAL NAME 'dk.aau.modelardb.engines.derby.$className'
       |""".stripMargin
  }

  def restrictionToSQLPredicates(restriction: Restriction, sgc: Array[Int], idc: HashMap[String, HashMap[Object, Array[Integer]]]): String = {
    restriction match {
      //NO PREDICATES (and IN...)
      case null => ""
      //COLUMN OPERATOR VALUE
      case col: Restriction.ColumnQualifier =>
        val name = col.getColumnName
        val op = col.getComparisonOperator
        val value = col.getConstantOperand
        (name, op, value) match {
          //SID
          case ("SID", ORDER_OP_EQUALS, value) => s" GID = " + PredicatePushDown.sidPointToGidPoint(value.asInstanceOf[Int], sgc)
          //TIMESTAMP
          case ("TIMESTAMP", ORDER_OP_GREATERTHAN, value) => " END_TIME > " + PredicatePushDown.toLongThroughTimestamp(value)
          case ("TIMESTAMP", ORDER_OP_GREATEROREQUALS, value) => "END_TIME >= " + PredicatePushDown.toLongThroughTimestamp(value)
          case ("TIMESTAMP", ORDER_OP_LESSTHAN, value) => " START_TIME < " + PredicatePushDown.toLongThroughTimestamp(value)
          case ("TIMESTAMP", ORDER_OP_LESSOREQUALS, value) => "START_TIME <= " + PredicatePushDown.toLongThroughTimestamp(value)
          case ("TIMESTAMP", ORDER_OP_EQUALS, value) => "(START_TIME <= " + PredicatePushDown.toLongThroughTimestamp(value) +
            " AND END_TIME >= " + PredicatePushDown.toLongThroughTimestamp(value) + ")"
          //DIMENSIONS
          case (column, ORDER_OP_EQUALS, value) if idc.containsKey(column) =>
            PredicatePushDown.dimensionEqualToGidIn(column, value, idc).mkString("GID IN (", ",", ")")
          case p => Static.warn("ModelarDB: unsupported predicate " + p, 120); ""
        }
      //AND
      case and: Restriction.AND =>
        "(" + restrictionToSQLPredicates(and.getLeftChild, sgc, idc) + " AND " + restrictionToSQLPredicates(and.getRightChild, sgc, idc) + ")"
      //OR
      case or: Restriction.OR =>
        "(" + restrictionToSQLPredicates(or.getLeftChild, sgc, idc) + " OR " + restrictionToSQLPredicates(or.getRightChild, sgc, idc) + ")"
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
        //LONG VARCHAR is not used as comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'CHAR (UCS_BASIC)' are not
        // supported, and columns of type 'LONG VARCHAR' may not be used in CREATE INDEX, ORDER BY, GROUP BY, UNION,
        // INTERSECT, EXCEPT or DISTINCT statements because comparisons are not supported for that type.
        case (name, Types.TEXT) => name + " VARCHAR(256)"
      }.mkString(", ", ", ", "")
    }
  }
}