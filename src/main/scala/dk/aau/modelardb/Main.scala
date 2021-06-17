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
package dk.aau.modelardb

import com.typesafe.scalalogging.Logger
import dk.aau.modelardb.akka.AkkaSystem
import dk.aau.modelardb.arrow.ArrowFlightServer
import dk.aau.modelardb.config.Config
import dk.aau.modelardb.engines.EngineFactory
import dk.aau.modelardb.storage.StorageFactory
import pureconfig._
import pureconfig.generic.auto._
import dk.aau.modelardb.config.ModelarConfig._

import java.util.TimeZone


object Main {

  private val log = Logger(Main.getClass)

  /** Public Methods **/
  def main(args: Array[String]): Unit = {

    /* Configuration */

    // Check args(0) for a config and use $HOME/modelardb.conf as a fallback
    val fallback = System.getProperty("user.home") + "/modelardb.conf"
    val configPath: String = if (args.length == 1) {
      args(0)
    } else if (new java.io.File(fallback).exists) {
      fallback
    } else {
      println("usage: modelardb path/to/modelardb.conf")
      System.exit(-1)
      "" //HACK: necessary to have the same type in all branches of the match expression
    }

    val baseConfig = ConfigSource.default
    val userConfig = ConfigSource.file(configPath)
    val config = userConfig
      .withFallback(baseConfig)
      .loadOrThrow[Config]

    val modelarConf = config.modelarDb
    TimeZone.setDefault(config.modelarDb.timezone) //Ensures all components use the same time zone


    /* Storage */
    val storage = StorageFactory.getStorage(modelarConf.storage)

    val akkaSystem = AkkaSystem(config, storage)

    /* Engine */
    val engine = EngineFactory.getEngine(modelarConf, storage)
    val queue = akkaSystem.start
    engine.start(queue)

    /* Interfaces */
    val arrowServer = ArrowFlightServer(config.arrow, engine, storage)
    arrowServer.start()

    Interface.start(config, engine)

    /* Cleanup */
    arrowServer.stop()
    engine.stop()
    storage.close()
    akkaSystem.stop()
    println("goodbye!")
  }
}
