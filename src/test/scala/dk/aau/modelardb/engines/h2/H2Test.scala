/* Copyright 2021 The ModelarDB Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dk.aau.modelardb.engines.h2

import dk.aau.modelardb.core.models.ModelTypeFactory
import dk.aau.modelardb.core.{Configuration, Dimensions, SegmentGroup}
import dk.aau.modelardb.engines.EngineUtilities
import dk.aau.modelardb.storage.JDBCStorage
import org.h2.expression.condition.{Comparison, ConditionAndOr}
import org.h2.jdbc.JdbcSQLDataException
import org.h2.table.TableFilter
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{DriverManager, Statement}
import java.time.Instant
import scala.collection.mutable

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

  behavior of "H2 Companion Object"
  it should "be able to make compareType and getCompareOperator public" in {
    val compareTypeField = classOf[Comparison].getDeclaredField("compareType")
    compareTypeField.setAccessible(true)
    val compareTypeMethod = classOf[Comparison].getDeclaredMethod("getCompareOperator", classOf[Int])
    compareTypeMethod.setAccessible(true)
    val andOrTypeField = classOf[ConditionAndOr].getDeclaredField("andOrType")
    andOrTypeField.setAccessible(true)
  }


  behavior of "ViewSegment"

  it should "be able to execute simple select" in {
    withH2 { statement =>
      val gid = 1
      val mtid = 1
      val tid = 1
      val samplingInterval = 10
      val startTime = Instant.ofEpochMilli(100L)
      val endTime = Instant.ofEpochMilli(110L)
      val sg = new SegmentGroup(gid, startTime.toEpochMilli, endTime.toEpochMilli, mtid, Array(0x42.toByte), Array(0x42.toByte))
      val model = ModelTypeFactory.getFallbackModelType(5.0f, 300)
      val dimensions = new Dimensions(Array())
      EngineUtilities.initialize(dimensions)

      val storage = mock[JDBCStorageNoArgs]
      (storage.getSegmentGroups(_: TableFilter))
        .expects(*)
        .returns(Iterator(sg))

      storage.timeSeriesGroupCache = Array(0, 1)
      storage.groupMetadataCache = Array(Array(), Array(samplingInterval, 1, 1), Array(samplingInterval, 1, 1))
      storage.groupDerivedCache = mutable.HashMap[Integer, Array[Int]]()
      storage.modelTypeCache = Array(model, model)
      storage.timeSeriesMembersCache = Array(null, Array())

      val configuration = new Configuration()
      configuration.add("modelardb.batch_size", 500)
      val h2 = new H2(configuration, storage)
      H2.initialize(h2, storage)

      statement.execute(H2.getCreateSegmentViewSQL(dimensions))
      val rs = statement.executeQuery("SELECT * FROM Segment")
      var count = 0
      while (rs.next()) {
        count += 1
        rs.getInt(1) should equal (tid)
        rs.getInt("tid") should equal (tid)
        rs.getTimestamp(2).toInstant should equal(startTime)
        rs.getTimestamp("start_time").toInstant should equal(startTime)
        rs.getTimestamp(3).toInstant should equal(endTime)
        rs.getTimestamp("end_time").toInstant should equal(endTime)
        rs.getInt(4) should equal (mtid)
        rs.getInt("mtid") should equal (mtid)

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
