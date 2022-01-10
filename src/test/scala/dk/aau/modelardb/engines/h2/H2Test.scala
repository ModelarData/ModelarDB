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
import dk.aau.modelardb.H2Provider
import dk.aau.modelardb.arrow.ArrowFlightClient
import dk.aau.modelardb.config.{ArrowClientConfig, ArrowConfig, Config, ModelarConfig}
import dk.aau.modelardb.core.{Dimensions, SegmentGroup}
import dk.aau.modelardb.engines.EngineUtilities
import dk.aau.modelardb.storage.{ArrayCache, JDBCStorage}
import org.h2.expression.condition.{Comparison, ConditionAndOr}
import org.h2.jdbc.JdbcSQLDataException
import org.h2.table.TableFilter
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigSource
import pureconfig.generic.auto._

import java.time.Instant
import scala.collection.mutable

class H2Test extends AnyFlatSpec with Matchers with MockFactory with H2Provider {

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
      val offset = 0
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

      storage.timeSeriesGroupCache = new ArrayCache[Int](2, offset)

      // Maps the gid of a group to the groups sampling interval and the tids that are part of that group
      storage.groupMetadataCache = new ArrayCache[Array[Int]](2, 0)
      storage.groupMetadataCache.set(gid, Array(samplingInterval, tid))

      // Maps the gid of a group to pairs of tids for time series with derived time series
      storage.groupDerivedCache = mutable.HashMap[Integer, Array[Int]]()
      storage.groupDerivedCache.put(gid, Array())

      storage.modelTypeCache = Array(model, model)
      storage.timeSeriesMembersCache = new ArrayCache[Array[AnyRef]](2,0)

      import ModelarConfig.timezoneReader // needed to read config
      val config = ConfigSource.resources("application-test.conf").loadOrThrow[Config]
      val modelarConfig = config.modelarDb
      val arrowFlightClient = ArrowFlightClient(config.arrow)
      val h2 = new H2(modelarConfig, storage, arrowFlightClient)
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
      count should equal(1)
    }
  }

  /* HACK: Needed because JDBCStorage class init fails when constructor arg is null */
  private class JDBCStorageNoArgs extends JDBCStorage("jdbc:h2:mem", 0)

}
