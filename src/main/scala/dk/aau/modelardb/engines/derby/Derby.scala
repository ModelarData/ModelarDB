package dk.aau.modelardb.engines.derby

import java.sql.{DriverManager, SQLException, Timestamp}
import dk.aau.modelardb.Interface
import dk.aau.modelardb.core.{Configuration, Storage}
import dk.aau.modelardb.engines.RDBMSEngineUtilities
import dk.aau.modelardb.engines.derby.Derby._
import org.apache.derby.vti.Restriction
import org.apache.derby.vti.Restriction.ColumnQualifier
import org.apache.derby.vti.Restriction.ColumnQualifier.{ORDER_OP_EQUALS => EQUALS, ORDER_OP_GREATEROREQUALS => GREATEROREQUALS, ORDER_OP_GREATERTHAN => GREATERTHAN, ORDER_OP_ISNOTNULL => ISNOTNULL, ORDER_OP_ISNULL => ISNULL, ORDER_OP_LESSOREQUALS => LESSOREQUALS, ORDER_OP_LESSTHAN => LESSTHAN, ORDER_OP_NOT_EQUALS => NOT_EQUALS}

import java.lang.ClassCastException
import java.time.Instant
import scala.annotation.tailrec

class Derby(configuration: Configuration, storage: Storage) {
  /** Public Methods **/
  def start(): Unit = {
    //Initialize
    //https://db.apache.org/derby/docs/10.15/security/rsecpolicysample.html
    //https://db.apache.org/derby/docs/10.15/devguide/cdevdvlpinmemdb.html
    val connection = DriverManager.getConnection("jdbc:derby:memory:;create=true")
    val stmt = connection.createStatement()

    //TODO: extend the schema of both views with the columns of the user-defined dimensions at run-time
    //https://db.apache.org/derby/docs/10.15/ref/rrefcreatefunctionstatement.html
    //https://db.apache.org/derby/docs/10.15/ref/rrefsqljexternalname.html
    //https://db.apache.org/derby/docs/10.15/ref/rrefsqlj15446.html
    stmt.execute(CreateSegmentFunctionSQL)
    stmt.execute(CreateSegmentViewSQL)

    stmt.execute(CreateDatapointFunctionSQL)
    stmt.execute(CreateDatapointViewSQL)

    //https://db.apache.org/derby/docs/10.15/ref/rrefsqljcreatetype.html
    //https://db.apache.org/derby/docs/10.15/ref/rrefsqljexternalname.html
    stmt.execute(CreateSegmentTypeSQL)
    //https://db.apache.org/derby/docs/10.15/ref/rrefcreatefunctionstatement.html
    //https://db.apache.org/derby/docs/10.15/ref/rrefsqljexternalname.html
    stmt.execute(CreateToSegmentFunctionSQL)
    //https://db.apache.org/derby/docs/10.15/ref/rrefsqljcreateaggregate.html
    //https://db.apache.org/derby/docs/10.15/ref/rrefsqljexternalname.html
    stmt.execute(CreateCountUDFSQL)
    stmt.close()

    //Ingestion
    RDBMSEngineUtilities.initialize(configuration, storage)
    val utilities = RDBMSEngineUtilities.getUtilities
    utilities.startIngestion()

    //Interface
    Interface.start(
      configuration,
      q => utilities.executeQuery(connection, q)
    )

    //Shutdown
    connection.close()
    RDBMSEngineUtilities.waitUntilIngestionIsDone()
  }
}

object Derby {

  val CreateSegmentFunctionSQL =
    """CREATE FUNCTION Segment()
      |RETURNS TABLE (sid INT, start_time TIMESTAMP, end_time TIMESTAMP, resolution INT, mid INT, parameters BLOB, gaps BLOB)
      |LANGUAGE JAVA
      |PARAMETER STYLE DERBY_JDBC_RESULT_SET
      |READS SQL DATA
      |EXTERNAL NAME 'dk.aau.modelardb.engines.derby.ViewSegment.apply'
      |""".stripMargin

  val CreateSegmentViewSQL = "CREATE VIEW Segment as SELECT s.* FROM TABLE(Segment()) s"

  val CreateSegmentTypeSQL =
    """CREATE TYPE segment
      |EXTERNAL NAME 'dk.aau.modelardb.engines.derby.Segment'
      |LANGUAGE JAVA
      |""".stripMargin

  val CreateDatapointFunctionSQL =
    """|CREATE FUNCTION DataPoint()
       |RETURNS TABLE (sid INT, timestamp TIMESTAMP, value REAL)
       |LANGUAGE JAVA PARAMETER STYLE DERBY_JDBC_RESULT_SET
       |READS SQL DATA
       |EXTERNAL NAME 'dk.aau.modelardb.engines.derby.ViewDataPoint.apply'
       |""".stripMargin

  val CreateDatapointViewSQL = "CREATE VIEW DataPoint as SELECT d.* FROM TABLE(DataPoint()) d"

  val CreateToSegmentFunctionSQL =
    """CREATE FUNCTION TO_SEGMENT(sid INT, start_time BIGINT, end_time BIGINT, resolution INT, mid INT, parameters BLOB, gaps BLOB)
      |RETURNS segment
      |PARAMETER STYLE JAVA NO SQL
      |LANGUAGE JAVA
      |EXTERNAL NAME 'dk.aau.modelardb.engines.derby.Segment.toSegment'""".stripMargin

  val CreateCountUDFSQL = "CREATE DERBY AGGREGATE count_s FOR segment EXTERNAL NAME 'dk.aau.modelardb.engines.derby.CountS'"


  def toLong(columnValue: AnyRef): Long = {
    columnValue match {
      case ts: Timestamp => ts.getTime
      case str: String => Timestamp.valueOf(str).getTime
      case _ => throw new SQLException(s"Unable to cast ${columnValue.getClass} to java.sql.Timestamp")
    }
  }

  def lookupOperator(opCode: Int): String = {
    opCode match {
      case EQUALS => "="
      case GREATEROREQUALS => ">="
      case GREATERTHAN => ">"
      case ISNOTNULL => "IS NOT NULL"
      case ISNULL => "IS NULL"
      case LESSOREQUALS => "<="
      case LESSTHAN => "<"
      case NOT_EQUALS => "!="
    }
  }

  def toSQL(restriction: Restriction, storage: Storage): String = {

    def loop(restriction: Restriction): String = {
      restriction match {
        case col: Restriction.ColumnQualifier =>
          val name = col.getColumnName
          val op = col.getComparisonOperator
          val value = col.getConstantOperand

          (name.toUpperCase, op, value) match {
            case ("SID", EQUALS, value)  =>
              val sid = value.asInstanceOf[Int]
              val gid = storage.sourceGroupCache(sid)
              s"GID = $gid "
            case ("SID", _, _) => throw new SQLException("Only equals [=] is supported on column SID")
            case ("START_TIME", op, value) => s"START_TIME ${lookupOperator(op)} ${toLong(value)} "
            case ("END_TIME", op, value) => s"END_TIME ${lookupOperator(op)} ${toLong(value)} "
            case ("MID", _, _) => col.toSQL + " "
            case _ =>  throw new SQLException(s"Unsupported column $name. Only SID, MID, START_TIME, END_TIME is supported in WHERE clause")
          }

        case and: Restriction.AND =>
          loop(and.getLeftChild) + " AND " + loop(and.getRightChild)

        case or: Restriction.OR =>
          loop(or.getLeftChild) + " OR " + loop(or.getRightChild)
      }
    }

    if (restriction == null) {
      "SELECT * FROM segment"
    } else {
      "SELECT * FROM segment WHERE " + loop(restriction)
    }
  }

  val OperatorSymbols = Array("<", "=", "<=", ">", ">=", "IS NULL", "IS NOT NULL", "!=")
}