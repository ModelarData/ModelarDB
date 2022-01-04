/* Copyright 2018 The ModelarDB Contributors
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

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import dk.aau.modelardb.config.Config
import java.net.{ServerSocket, InetSocketAddress}
import dk.aau.modelardb.core.utility.Static
import dk.aau.modelardb.engines.QueryEngine
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter

//import java.io.OutputStream
import java.nio.charset.StandardCharsets.UTF_8
import java.io.{BufferedReader, InputStreamReader, OutputStream}
import java.nio.file.{Files, Paths}
import java.util.concurrent.{Executor, Executors}
import scala.io.Source
import scala.io.StdIn.readLine

object Interface {

  /** Instance Variables **/
  var queryEngine: QueryEngine = _
  val executor = Executors.newCachedThreadPool()
  var humanFriendly = false

  /** Public Methods **/
  def start(config: Config, queryEngine: QueryEngine): Unit = {
    humanFriendly = config.modelarDb.human
    this.queryEngine = queryEngine
    val (interface, port) = getInterfaceAndPort(config)
    interface match {
      case "socket" => socket(executor)
      case "http" => http(executor)
      case "repl" => repl(config.modelarDb.storage)
      case path if Files.exists(Paths.get(path)) => file(path)
      case _ => throw new java.lang.UnsupportedOperationException("unknown value for modelardb.interface in the config file")
    }
  }

  /** Private Methods **/
  private def getInterfaceAndPort(config: Config): (String, Int) = {
    val interface = config.modelarDb.interface
    val startOfPort = interface .lastIndexOf(':')
    if (startOfPort == -1) {
      (interface, 9999)
    } else {
      (interface.substring(0, startOfPort), interface.substring(startOfPort + 1).toInt)
    }
  }

  private def socket(executor: Executor, port: Int, sql: String => Array[String]): Unit = {
    //Setup
    val serverSocket = new ServerSocket(port)
    Static.info(s"ModelarDB: socket end-point is ready (Port: $port)")

    while (true) {
      val clientSocket = serverSocket.accept()
      executor.execute(() => {
        val in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream))
        val out = clientSocket.getOutputStream

        //Query
        Static.info("ModelarDB: connection is ready")

        try {
          var stop = true
          while (stop) {
            val query = in.readLine().trim()
            if ( ! query.startsWith("--") && query.contains("SELECT")) {
              execute(query, out)
              out.flush()
            } else if (query.nonEmpty) {
              in.close()
              out.close()
              clientSocket.close()
              stop = false //The empty string terminates the connection
              Static.info("ModelarDB: conection is closed")
            } else {
              out.write("only SELECT is supported".getBytes(UTF_8))
              out.flush()
            }
          }
        } catch {
          case _: NullPointerException =>
            //Thrown if the client closes in while ModelarDB waits for input
        }
      })
    }

    //Cleanup
    serverSocket.close()
    Static.info(s"ModelarDB: socket end-point is closed (Port: $port)")
  }

  private def http(executor: Executor, port: Int, sql: String => Array[String]): Unit = {
    //Setup
    val server = HttpServer.create(new InetSocketAddress(port), 0)

    //Query
    class QueryHandler extends HttpHandler {
      override def handle(httpExchange: HttpExchange): Unit = {
        val request = httpExchange.getRequestBody
        val reader = new BufferedReader(new InputStreamReader(request))

        httpExchange.sendResponseHeaders(200, 0)
        val response = httpExchange.getResponseBody
        execute(reader.readLine.trim(), response)
        response.close()
      }
    }

    //Configures the HTTP server to executes QueryHandler on a separate thread for each incoming request on /
    server.createContext("/", new QueryHandler())
    server.setExecutor(executor)
    server.start()
    Static.info("ModelarDB: HTTP end-point is ready (Port: $port)")
    scala.io.StdIn.readLine() //Prevents the method from returning to keep the server running

    //Cleanup
    server.stop(0)
    Static.info(s"ModelarDB: HTTP end-point is closed (Port: $port)")
  }

  private def repl(storage: String): Unit = {
    val prompt = storage.substring(storage.lastIndexOf('/') + 1) + "> "
    do {
      print(prompt)
      execute(readLine, print)
    } while(true)
  }

  private def file(path: String): Unit = {
    //This method is only called if the file exist
    val st = System.currentTimeMillis()
    Static.info("ModelarDB: executing queries from " + path)
    val source = Source.fromFile(path)
    for (line: String <- source.getLines()) {
      val q = line.trim()
      if ( ! (q.isEmpty || q.startsWith("--"))) {
        execute(q.stripMargin, print)
      }
    }
    source.close()
    val et = System.currentTimeMillis() - st
    val jst = java.time.Duration.ofMillis(et)
    Static.info("ModelarDB: finished all queries after " + jst)
  }

  private def execute(query: String, out: OutputStream): Unit = {
    val st = System.currentTimeMillis()
    val result: VectorSchemaRoot = try {
      val query_rewritten = query
        .replace("COUNT_S(#)", "COUNT_S(tid, start_time, end_time)")
        .replace("#", "tid, start_time, end_time, mtid, model, gaps")
      queryEngine.execute(query_rewritten)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        return
    }

    // Outputs the query result using the method provided as the arguments `out`
    val et = System.currentTimeMillis() - st
    val jst = java.time.Duration.ofMillis(et)
    if (humanFriendly) {
      val header = s"""time: "$jst",\nquery: "$query",\nresult:\n""".getBytes(UTF_8)
      out.write(header)
      // TODO: use different formatting than TSV as this makes JSON response look strange
      val bytes = result.contentToTSVString().getBytes(UTF_8)
      out.write(bytes)
    } else {
      val arrowWriter = new ArrowStreamWriter(result, /*DictionaryProvider*/ null, out)
      arrowWriter.start()
      arrowWriter.writeBatch()
      arrowWriter.end()
    }
  }

}
