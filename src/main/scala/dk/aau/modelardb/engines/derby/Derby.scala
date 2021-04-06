package dk.aau.modelardb.engines.derby

import java.sql.DriverManager
import dk.aau.modelardb.Interface
import dk.aau.modelardb.core.Dimensions.Types
import dk.aau.modelardb.core.utility.Static
import dk.aau.modelardb.core.{Configuration, Dimensions, Storage}
import dk.aau.modelardb.engines.{PredicatePushDown, RDBMSEngineUtilities}
import dk.aau.modelardb.engines.derby.Derby._
import org.apache.derby.vti.Restriction
import org.apache.derby.vti.Restriction.ColumnQualifier.{ORDER_OP_EQUALS, ORDER_OP_GREATEROREQUALS, ORDER_OP_GREATERTHAN, ORDER_OP_ISNOTNULL, ORDER_OP_ISNULL, ORDER_OP_LESSOREQUALS, ORDER_OP_LESSTHAN, ORDER_OP_NOT_EQUALS}

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

    /* Documentation:
     * https://db.apache.org/derby/docs/10.15/ref/rrefsqljcreatetype.html
     * https://db.apache.org/derby/docs/10.15/ref/rrefsqljexternalname.html */
    stmt.execute(CreateSegmentTypeSQL)
    /* Documentation:
     * https://db.apache.org/derby/docs/10.15/ref/rrefcreatefunctionstatement.html
     * https://db.apache.org/derby/docs/10.15/ref/rrefsqljexternalname.html */
    stmt.execute(CreateToSegmentFunctionSQL)
    /* Documentation:
     * https://db.apache.org/derby/docs/10.15/ref/rrefsqljcreateaggregate.html
     * https://db.apache.org/derby/docs/10.15/ref/rrefsqljexternalname.html */
    stmt.execute(CreateCountUDAFSQL)
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

  /** Public Methods and Variables **/
  //Data Point View
  def getCreateDataPointFunctionSQL(dimensions: Dimensions): String = {
    s"""CREATE FUNCTION DataPoint()
       |RETURNS TABLE (sid INT, timestamp TIMESTAMP, value REAL${Derby.getDimensionColumns(dimensions)})
       |LANGUAGE JAVA PARAMETER STYLE DERBY_JDBC_RESULT_SET
       |READS SQL DATA
       |EXTERNAL NAME 'dk.aau.modelardb.engines.derby.ViewDataPoint.apply'
       |""".stripMargin
  }

  val CreateDataPointViewSQL = "CREATE VIEW DataPoint as SELECT d.* FROM TABLE(DataPoint()) d"

  //Segment View
  def getCreateSegmentFunctionSQL(dimensions: Dimensions): String = {
    s"""CREATE FUNCTION Segment()
       |RETURNS TABLE (sid INT, start_time TIMESTAMP, end_time TIMESTAMP, resolution INT, mid INT, parameters LONG VARCHAR FOR BIT DATA, gaps LONG VARCHAR FOR BIT DATA${Derby.getDimensionColumns(dimensions)})
       |LANGUAGE JAVA
       |PARAMETER STYLE DERBY_JDBC_RESULT_SET
       |READS SQL DATA
       |EXTERNAL NAME 'dk.aau.modelardb.engines.derby.ViewSegment.apply'
       |""".stripMargin
  }

  val CreateSegmentViewSQL = "CREATE VIEW Segment as SELECT s.* FROM TABLE(Segment()) s"

  //Segment View UDAFs
  val CreateSegmentTypeSQL: String =
    """CREATE TYPE segment
      |EXTERNAL NAME 'dk.aau.modelardb.engines.derby.Segment'
      |LANGUAGE JAVA
      |""".stripMargin

  val CreateToSegmentFunctionSQL: String =
    """CREATE FUNCTION TO_SEGMENT(sid INT, start_time TIMESTAMP, end_time TIMESTAMP, resolution INT, mid INT, parameters LONG VARCHAR FOR BIT DATA, gaps LONG VARCHAR FOR BIT DATA)
      |RETURNS segment
      |PARAMETER STYLE JAVA NO SQL
      |LANGUAGE JAVA
      |EXTERNAL NAME 'dk.aau.modelardb.engines.derby.Segment.toSegment'""".stripMargin

  val CreateCountUDAFSQL: String = "CREATE DERBY AGGREGATE count_s FOR segment EXTERNAL NAME 'dk.aau.modelardb.engines.derby.CountS'"


  def restrictionToSQLPredicates(restriction: Restriction, sgc: Array[Int]): String = {
    //TODO: Determine if Derby really does not support predicate push-down for IN clauses
    if (restriction == null) {
      ""
    } else {
      restriction match {
        case col: Restriction.ColumnQualifier =>
          val name = col.getColumnName
          val op = col.getComparisonOperator
          val value = col.getConstantOperand

          (name, op, value) match {
            //SID
            case ("SID", ORDER_OP_EQUALS, value) => s" GID = " + PredicatePushDown.sidPointToGidPoint(value.asInstanceOf[Int], sgc)
            //TIMESTAMP
            //START TIME
            case ("START_TIME", op, value) => s"START_TIME ${lookupOperator(op)} ${PredicatePushDown.toLongThroughTimestamp(value)} "
            //END TIME
            case ("END_TIME", op, value) => s"END_TIME ${lookupOperator(op)} ${PredicatePushDown.toLongThroughTimestamp(value)} "
            //DIMENSIONS
            case p => Static.warn("ModelarDB: unsupported predicate " + p, 120); ""
          }
        case and: Restriction.AND =>
          restrictionToSQLPredicates(and.getLeftChild, sgc) + " AND " + restrictionToSQLPredicates(and.getRightChild, sgc)
        case or: Restriction.OR =>
          restrictionToSQLPredicates(or.getLeftChild, sgc) + " OR " + restrictionToSQLPredicates(or.getRightChild, sgc)
        case p => Static.warn("ModelarDB: unsupported predicate " + p, 120); ""
      }
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
        case (name, Types.TEXT) => name + " LONG VARCHAR"
      }.mkString(", ", ", ", "")
    }
  }

  private def lookupOperator(opCode: Int): String = {
    opCode match {
      case ORDER_OP_EQUALS => "="
      case ORDER_OP_GREATEROREQUALS => ">="
      case ORDER_OP_GREATERTHAN => ">"
      case ORDER_OP_ISNOTNULL => "IS NOT NULL"
      case ORDER_OP_ISNULL => "IS NULL"
      case ORDER_OP_LESSOREQUALS => "<="
      case ORDER_OP_LESSTHAN => "<"
      case ORDER_OP_NOT_EQUALS => "!="
    }
  }
}