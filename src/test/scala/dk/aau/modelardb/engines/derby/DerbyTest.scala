package dk.aau.modelardb.engines.derby

import dk.aau.modelardb.core.models.ModelFactory
import dk.aau.modelardb.core.timeseries.TimeSeriesCSV
import dk.aau.modelardb.core.utility.{Pair, ValueFunction}
import dk.aau.modelardb.core.{Configuration, Correlation, Dimensions, SegmentGroup, TimeSeriesGroup}
import dk.aau.modelardb.engines.RDBMSEngineUtilities
import dk.aau.modelardb.storage.{JDBCStorage, StorageFactory}
import org.apache.derby.vti.Restriction
import org.scalamock.matchers.ArgCapture.CaptureOne
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Path, Paths}
import java.sql.{DriverManager, SQLDataException, SQLException, Statement, Timestamp}
import java.time.Instant
import scala.collection.JavaConverters._
import scala.io.Source

class DerbyTest extends AnyFlatSpec with MockFactory with Matchers {

  private val ConnectionString = "jdbc:derby:memory:testdb;create=true"

  private def withDerby[A](fun: Statement => A): A = {
    val conn = DriverManager.getConnection(ConnectionString)
    try {
      val statement = conn.createStatement()
      fun(statement)
    } finally {
      conn.close()

      /** Derby quirk: the way you drop the in-memory table is using a connect string attribute.
       * A successful drop is indicated by an exception with SQL error: 08006
       * This is needed to make sure individual tests do not interfere with each other.
       * https://db.apache.org/derby/docs/10.15/devguide/cdevdvlpinmemdb.html
       * https://db.apache.org/derby/docs/10.15/getstart/rwwdactivity3.html */
      try {
        DriverManager.getConnection("jdbc:derby:memory:testdb;drop=true")
      } catch {
        case ex: SQLException if ex.getSQLState == "08006" =>
        case throwable: Throwable => throw throwable
      }

    }
  }

  /* HACK: needed because JDBCStorage class init fails when constructor arg is null */
  private class JDBCStorageNoArgs extends JDBCStorage(ConnectionString)


  behavior of "ViewSegment"

  it should "be able to execute simple select" in {
    withDerby { statement =>
      val gid = 1
      val mid = 1
      val sid = 1
      val resolution = 10
      val startTime = Instant.ofEpochMilli(100L)
      val endTime = Instant.ofEpochMilli(110L)
      val sg = new SegmentGroup(gid, startTime.toEpochMilli, endTime.toEpochMilli, mid, Array(0x42.toByte), Array(0x42.toByte))
      val model = ModelFactory.getFallbackModel(5.0f, 300)

      val storage = mock[JDBCStorageNoArgs]
      storage.groupMetadataCache = Array(Array(), Array(resolution, 1, 1), Array(resolution, 1, 1))
      storage.groupDerivedCache = new java.util.HashMap[Integer, Array[Int]]()
      storage.modelCache = Array(model, model)

      val dimensions = new Dimensions(Array())

      (storage.getSegmentGroups(_: Restriction))
        .expects(*)
        .returns(Iterator(sg))

      val config = new Configuration()
      config.add("modelardb.batch", 1)
      config.add("modelardb.dimensions", Array(dimensions))

      RDBMSEngineUtilities.initialize(config, storage)

      statement.execute(Derby.CreateSegmentFunctionSQL)
      statement.execute(Derby.CreateSegmentViewSQL)

      val rs = statement.executeQuery("""SELECT * FROM segment""")
      var count = 0
      while (rs.next()) {
        count += 1

        rs.getInt(1) should equal (sid)
        rs.getTimestamp(2).toInstant should equal(startTime)
        rs.getTimestamp(3).toInstant should equal(endTime)
        rs.getInt(4) should equal (resolution)

        an [SQLDataException] should be thrownBy rs.getInt(2)
        an [SQLDataException] should be thrownBy rs.getInt(3)
        an [SQLDataException] should be thrownBy rs.getTimestamp(1)
        an [SQLDataException] should be thrownBy rs.getTimestamp(4)
      }
      count should equal(2)
    }
  }


  it should "work with predicate pushdown" in {
    withDerby { statement =>
      val gid = 1
      val mid = 1
      val sid = 1
      val resolution = 10
      val startTime = Instant.ofEpochMilli(100L)
      val endTime = Instant.ofEpochMilli(110L)
      val sg = new SegmentGroup(gid, startTime.toEpochMilli, endTime.toEpochMilli, mid, Array(0x42.toByte), Array(0x42.toByte))
      val model = ModelFactory.getFallbackModel(5.0f, 300)

      val storage = mock[JDBCStorageNoArgs]

      val c1 = CaptureOne[Restriction]()
      (storage.getSegmentGroups(_: Restriction))
        .expects(capture(c1))
        .returns(Iterator(sg))

      storage.groupMetadataCache = Array(Array(), Array(resolution, 1, 1), Array(resolution, 1, 1))
      storage.groupDerivedCache = new java.util.HashMap[Integer, Array[Int]]()
      storage.modelCache = Array(model, model)
      storage.sourceGroupCache = Array(gid, gid)

      val config = new Configuration()
      config.add("modelardb.batch", 1)

      RDBMSEngineUtilities.initialize(config, storage)

      statement.execute(Derby.CreateSegmentFunctionSQL)
      statement.execute(Derby.CreateSegmentViewSQL)

      val inputSql = s"""SELECT *
                  |FROM segment
                  |WHERE sid = 1
                  |AND start_time BETWEEN '${Timestamp.from(startTime).toString}' AND '${Timestamp.from(endTime).toString}'
                  |""".stripMargin
      statement.executeQuery(inputSql)

      val restriction = c1.value
      val outputSql = Derby.toSQL(restriction, storage)

      outputSql should startWith ("SELECT * FROM segment")
      outputSql should include ("GID = 1")
      outputSql should include (s"START_TIME >= ${startTime.toEpochMilli}")
      outputSql should include (s"START_TIME <= ${endTime.toEpochMilli}")

    }
  }
}
