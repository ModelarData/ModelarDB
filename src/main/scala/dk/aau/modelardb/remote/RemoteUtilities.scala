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
package dk.aau.modelardb.remote

import org.apache.arrow.memory.BufferAllocator

import dk.aau.modelardb.core.Configuration

object RemoteUtilities {

  /** Public Methods **/
  def rewriteQuery(query: String): String = {
    query.replace("COUNT_S(#)", "COUNT_S(tid, start_time, end_time)")
      .replace("#", "tid, start_time, end_time, mtid, model, offsets")
  }

  def getInterfaceAndPort(interfaceAndMaybePort : String, defaultPort: Int): (String, Int) = {
    val startOfPort = interfaceAndMaybePort.lastIndexOf(':')
    if (startOfPort == -1) {
      (interfaceAndMaybePort, defaultPort)
    } else {
      (interfaceAndMaybePort.substring(0, startOfPort), interfaceAndMaybePort.substring(startOfPort + 1).toInt)
    }
  }

  def getRootAllocator(configuration: Configuration): BufferAllocator = {
    configuration.get("modelardb.root_allocator")(0).asInstanceOf[BufferAllocator]
  }
}