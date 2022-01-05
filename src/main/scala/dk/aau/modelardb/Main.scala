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
import dk.aau.modelardb.arrow.{ArrowFlightClient, ArrowFlightServer}
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

    // ModelarDB checks args(0) for a config and uses $HOME/modelardb.conf as a fallback
    val fallback = System.getProperty("user.home") + "/modelardb.conf"
    val configPath: String = if (args.length == 1) {
      args(0)
    } else {
      fallback
    }

    val baseConfig = ConfigSource.default
    val userConfig = ConfigSource
      .file(configPath)
      .optional // returns empty config if configPath is unspecified by user
    val config = userConfig
      .withFallback(baseConfig)
      .loadOrThrow[Config]

    val modelarConf = config.modelarDb
    TimeZone.setDefault(config.modelarDb.timezone) //Ensures all components use the same time zone
    val mode = config.modelarDb.mode

    val arrowFlightClient = ArrowFlightClient(config.arrow)
    val tsCount = config.modelarDb.sources.length
    val tidOffset = mode.toLowerCase match {
      case "edge" => arrowFlightClient.getTidOffset(tsCount)
      case "server" => 0
      case _ => throw new Exception("Config parameter \"mode\" only support values {server, edge}")
    }

    /* Storage */
    val storage = StorageFactory.getStorage(modelarConf.storage, tidOffset)

    val akkaSystem = AkkaSystem(config, storage, arrowFlightClient)


    /* Engine */
    val engine = EngineFactory.getEngine(modelarConf, storage, arrowFlightClient)
    val queue = akkaSystem.start

    /* Interfaces */
    val arrowServer = ArrowFlightServer(config.arrow, engine, storage, mode)
    arrowServer.start()

    mode.toLowerCase match {
      case "edge" => engine.start(queue)
      case "server" => engine.start
      case _ => throw new Exception("Config parameter \"mode\" only support values {server, edge}")
    }

    Interface.start(config, engine)

    /* Cleanup */
    arrowServer.stop()
    engine.stop()
    storage.close()
    akkaSystem.stop()
    println("goodbye!")
  }
}
