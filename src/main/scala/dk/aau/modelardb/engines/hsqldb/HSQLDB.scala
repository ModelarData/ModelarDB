package dk.aau.modelardb.engines.hsqldb

import java.sql.DriverManager
import dk.aau.modelardb.Interface
import dk.aau.modelardb.core.{Configuration, Storage}
import dk.aau.modelardb.engines.RDBMSEngineUtilities

class HSQLDB(configuration: Configuration, storage: Storage) {
  /** Public Methods **/
  def start(): Unit = {
    //Engine
    //http://hsqldb.org/doc/2.0/guide/dbproperties-chapt.html#dpc_connection_url:w
    val connection = DriverManager.getConnection("jdbc:hsqldb:mem: ") //In-memory database named ' '
    val stmt = connection.createStatement()
    //http://hsqldb.org/doc/2.0/guide/sqlroutines-chapt.html#src_routine_definition
    stmt.execute( //HSQLDB cannot handle columns with the same names as types or keywords
    """
        |CREATE FUNCTION DataPoint()
        |RETURNS TABLE(sid INT, ts TIMESTAMP, val REAL)
        |READS SQL DATA
        |LANGUAGE JAVA
        |EXTERNAL NAME 'CLASSPATH:dk.aau.modelardb.engines.hsqldb.ViewDataPoint.queryView'
        |""".stripMargin)

    //http://hsqldb.org/doc/2.0/guide/databaseobjects-chapt.html#dbc_view_creation
    stmt.execute("CREATE VIEW DataPoint as SELECT * FROM TABLE(DataPoint())")

    //http://hsqldb.org/doc/2.0/guide/sqlroutines-chapt.html#src_aggregate_functions
    stmt.execute(
      """
        |CREATE AGGREGATE FUNCTION count_s(IN x INT ARRAY, IN flag BOOLEAN, INOUT addup BIGINT, INOUT counter INT)
        |    RETURNS INTEGER
        |    CONTAINS SQL
        |  BEGIN ATOMIC
        |  IF flag THEN
        |      RETURN counter;
        |  ELSE
        |      -- SET counter = COALESCE(counter, 0) + 1;
        |      -- SET addup = COALESCE(addup, 0) + COALESCE(x, 0);
        |      RETURN NULL;
        |  END IF;
        |END
        |""".stripMargin)
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
