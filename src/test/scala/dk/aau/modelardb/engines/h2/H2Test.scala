package dk.aau.modelardb.engines.h2

import dk.aau.modelardb.core.models.ModelFactory
import dk.aau.modelardb.core.{Configuration, SegmentGroup}
import dk.aau.modelardb.engines.RDBMSEngineUtilities
import dk.aau.modelardb.storage.JDBCStorage
import org.h2.jdbc.JdbcSQLDataException
import org.h2.table.TableFilter
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{DriverManager, Statement}
import java.time.Instant

class H2Test extends AnyFlatSpec with Matchers with MockFactory {

  private def withH2[A](fun: Statement => A): A = {
    val conn = DriverManager.getConnection("jdbc:h2:mem:")
    try {
      val statement = conn.createStatement()
      fun(statement)
    } finally {
      conn.close()
    }
  }

  behavior of "ViewSegment"

  it should "be able to execute simple select" in {
    withH2 { statement =>
      val gid = 1
      val mid = 1
      val sid = 1
      val resolution = 10
      val startTime = Instant.ofEpochMilli(100L)
      val endTime = Instant.ofEpochMilli(110L)
      val sg = new SegmentGroup(gid, startTime.toEpochMilli, endTime.toEpochMilli, mid, Array(0x42.toByte), Array(0x42.toByte))
      val model = ModelFactory.getFallbackModel(5.0f, 300)

      val storage = mock[JDBCStorageNoArgs]
      (storage.getSegmentGroups(_: TableFilter))
        .expects(*)
        .returns(Iterator(sg))

      storage.groupMetadataCache = Array(Array(), Array(resolution, 1, 1), Array(resolution, 1, 1))
      storage.groupDerivedCache = new java.util.HashMap[Integer, Array[Int]]()
      storage.modelCache = Array(model, model)

      val config = new Configuration()
      config.add("modelardb.batch", 1)
      RDBMSEngineUtilities.initialize(config, storage)

      statement.execute("""CREATE TABLE Segment
                          |(sid INT, start_time TIMESTAMP, end_time TIMESTAMP, resolution INT, mid INT, parameters BYTEA, gaps BYTEA)
                          |ENGINE "dk.aau.modelardb.engines.h2.ViewSegment";
                          |""".stripMargin)
      val rs = statement.executeQuery("SELECT * FROM segment")
      var count = 0
      while (rs.next()) {
        count += 1
        rs.getInt(1) should equal (sid)
        rs.getInt("sid") should equal (sid)
        rs.getTimestamp(2).toInstant should equal(startTime)
        rs.getTimestamp("start_time").toInstant should equal(startTime)
        rs.getTimestamp(3).toInstant should equal(endTime)
        rs.getTimestamp("end_time").toInstant should equal(endTime)
        rs.getInt(4) should equal (resolution)
        rs.getInt("resolution") should equal (resolution)

        an [JdbcSQLDataException] should be thrownBy rs.getInt(2)
        an [JdbcSQLDataException] should be thrownBy rs.getInt(3)
        an [JdbcSQLDataException] should be thrownBy rs.getTimestamp(1)
        an [JdbcSQLDataException] should be thrownBy rs.getTimestamp(4)
      }
      count should equal(2)
    }
  }

  /* HACK: needed because JDBCStorage class init fails when constructor arg is null */
  private class JDBCStorageNoArgs extends JDBCStorage("jdbc:h2:mem")
}
