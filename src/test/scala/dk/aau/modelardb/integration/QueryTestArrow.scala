/* Copyright 2022 The ModelarDB Contributors
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
package dk.aau.modelardb.integration

import org.apache.arrow.flight.{FlightClient, Location, Ticket}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, FieldVector, Float4Vector, Float8Vector, IntVector, TimeStampMicroTZVector, TimeStampMilliVector, VarBinaryVector}

import java.nio.charset.StandardCharsets

import scala.collection.{SortedMap, mutable}
import scala.collection.JavaConverters._

class QueryTestArrow extends QueryTest {

  override protected def getPorts: (Int, Int) = (9990, 9991)

  override protected def getInterface(port: Int): String = s"arrow:$port"

  override protected def executeQuery(query: String, port: Int): List[SortedMap[String, Object]] = {
    val client = FlightClient.builder().allocator(new RootAllocator())
      .location(Location.forGrpcInsecure("127.0.0.1", port)).build()

    val fs = client.getStream(new Ticket(query.getBytes(StandardCharsets.UTF_8)))
    val result = mutable.Buffer[SortedMap[String, Object]]()
    while (fs.next()) {
      val vsr = fs.getRoot
      val fvs = vsr.getFieldVectors.asScala
      for (index <- Range(0, vsr.getRowCount)) {
        val row = mutable.SortedMap[String, Object]()
        fvs.foreach(fv => row.put(fv.getName, this.get(fv, index)))
        result.append(row)
      }
      vsr.close()
    }
    fs.close()
    client.close()
    result.toList
  }

  /** Private Methods **/
  private def get(fv: FieldVector, index: Int): Object = {
    fv match {
      case fvi: IntVector => fvi.get(index).asInstanceOf[Object]
      case fvbi: BigIntVector => fvbi.get(index).asInstanceOf[Object]
      case fv4f: Float4Vector => fv4f.get(index).asInstanceOf[Object]
      case fv8f: Float8Vector => fv8f.get(index).asInstanceOf[Object]
      case fvtm: TimeStampMilliVector => fvtm.get(index).asInstanceOf[Object]
      case fvtmz: TimeStampMicroTZVector => (fvtmz.get(index) / 1000).asInstanceOf[Object]
      case fvvbv: VarBinaryVector => fvvbv.get(index).asInstanceOf[Object]
    }
  }
}