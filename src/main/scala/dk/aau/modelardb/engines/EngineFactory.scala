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
package dk.aau.modelardb.engines

import dk.aau.modelardb.core.{Configuration, Storage}
import dk.aau.modelardb.engines.h2.H2Storage
import dk.aau.modelardb.engines.spark.SparkStorage

import java.sql.Connection

object EngineFactory {

  /** Public Methods **/
  def getEngine(engineType: String, storage: Storage): QueryEngine = {
    //Extracts the name of the system from the engine connection string
    engineType.toLowerCase match {
      case "h2" => new dk.aau.modelardb.engines.h2.H2(engineType, storage.asInstanceOf[H2Storage])
      case "spark" => new dk.aau.modelardb.engines.spark.Spark(engineType, storage.asInstanceOf[SparkStorage])
      case _ =>
        throw new java.lang.UnsupportedOperationException("ModelarDB: unknown value for modelardb.engine in the config file")
    }
  }
}