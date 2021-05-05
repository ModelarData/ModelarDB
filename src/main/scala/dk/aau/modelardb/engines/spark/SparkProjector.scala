/* Copyright 2018-2020 Aalborg University
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
package dk.aau.modelardb.engines.spark

import dk.aau.modelardb.core.DataPoint
import dk.aau.modelardb.core.utility.ValueFunction
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

//Abstract classes that projections generated at run-time using the ToolBox APIs can derive from
abstract class SparkSegmentProjector {
  def project(row: Row, dmc: Array[Array[Object]]): Row
}

abstract class SparkDataPointProjector {
  def project(dp: DataPoint, dmc: Array[Array[Object]], sc: Array[Float], btc: Broadcast[Array[ValueFunction]]): Row
}

object SparkProjector {

  def getSegmentProjection(requiredColumns: Array[String], segmentViewNameToIndex: Map[String, Int]): String = {
    val columns = requiredColumns.map(column => {
      val index = segmentViewNameToIndex(column)
      if (index <= 7) "segmentRow.get(" + (index - 1) + ")" else "dmc(tid)(" + (index - 8) + ")"
    })

    val code = s"""
        import dk.aau.modelardb.engines.spark.SparkSegmentProjector
        import java.sql.Timestamp
        import org.apache.spark.sql.Row

        new SparkSegmentProjector {
            override def project(segmentRow: Row, dmc: Array[Array[Object]]): Row = {
              val tid = segmentRow.getInt(0)
              ${columns.mkString("Row(", ",", ")")}
            }
        }
    """.stripMargin
    code
  }

  def compileSegmentProjection(code: String): SparkSegmentProjector = {
    compileCode(code).asInstanceOf[SparkSegmentProjector]
  }

  def getDataPointProjection(requiredColumns: Array[String], dataPointViewNameToIndex: Map[String, Int]): String = {
    val columns = requiredColumns.map(column => {
      val index = dataPointViewNameToIndex(column)
      if (index <= 3) dataPointProjectionFragments(index - 1) else "dmc(dp.tid)(" + (index - 4) + ")"
    })

    val code = s"""
        import dk.aau.modelardb.engines.spark.SparkDataPointProjector
        import dk.aau.modelardb.core.DataPoint
        import java.sql.Timestamp
        import dk.aau.modelardb.core.utility.ValueFunction
        import org.apache.spark.broadcast.Broadcast
        import org.apache.spark.sql.Row

        new SparkDataPointProjector {
            override def project(dp: DataPoint, dmc: Array[Array[Object]], sc: Array[Float], btc: Broadcast[Array[ValueFunction]]): Row = {
                ${columns.mkString("Row(", ",", ")")}
            }
        }
    """.stripMargin
    code
  }

  def compileDataPointProjection(code: String): SparkDataPointProjector = {
    compileCode(code).asInstanceOf[SparkDataPointProjector]
  }

  def getValueFunction(transformation: String): ValueFunction = {
    val code = s"""
    import dk.aau.modelardb.core.utility.ValueFunction
    import scala.math._
    new ValueFunction() {
      override def transform(value: Float, scalingFactor: Float): Float = {
        return ($transformation).asInstanceOf[Float]
      }
    }"""
    compileCode(code).asInstanceOf[ValueFunction]
  }

  /** Private Methods **/
  private def compileCode(code: String): Any = {
    //Imports the packages required to construct the toolbox
    import scala.reflect.runtime.currentMirror
    import scala.tools.reflect.ToolBox
    val toolBox = currentMirror.mkToolBox()

    //Parses and compiles the code before constructing an object
    val ast = toolBox.parse(code)
    val compiled = toolBox.compile(ast)
    compiled()
  }

  /** Instance Variables **/
  private val dataPointProjectionFragments = Array("dp.tid", "new Timestamp(dp.timestamp)",
    "btc.value(dp.tid).transform(dp.value, sc(dp.tid))")
}
