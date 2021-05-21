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

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import dk.aau.modelardb.config.Config
import dk.aau.modelardb.core.utility.Static
import dk.aau.modelardb.engines.QueryEngine
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter

import java.io.OutputStream
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import scala.io.Source
import scala.io.StdIn.readLine

object Interface {

  /** Instance Variables **/
  var queryEngine: QueryEngine = _

  var humanFriendly = false

  /** Public Methods **/
  def start(config: Config, queryEngine: QueryEngine): Unit = {
    humanFriendly = config.modelarDb.human
    this.queryEngine = queryEngine
    config.modelarDb.interface.toLowerCase match {
      case "socket" => socket()
      case "http" => http()
      case "repl" => repl(config.modelarDb.storage)
      case path if Files.exists(Paths.get(path)) => file(path)
      case _ => throw new java.lang.UnsupportedOperationException("unknown value for modelardb.interface in the config file")
    }
  }

  /** Private Methods **/
  private def socket(): Unit = {
    //Setup
    Static.info("ModelarDB: preparing socket end-point (Port: 9999)")
    val serverSocket = new java.net.ServerSocket(9999)
    val clientSocket = serverSocket.accept()
    val in = new java.io.BufferedReader(new java.io.InputStreamReader(clientSocket.getInputStream))
    val out = clientSocket.getOutputStream

    //Query
    Static.info("ModelarDB: the socket is ready to receive queries (Port: 9999)")
    while (true) {
      val query = in.readLine().trim() // TODO: Handle ctrl-c (as of now it throws NPE because readLine becomes null
      if (query == ":quit") {
        Static.info("ModelarDB: received termination command, shutdown imminent")
        cleanUp()
        return
      } else if (query.nonEmpty) {
        execute(query, out)
        out.flush()
      }
    }
    cleanUp()

    //Cleanup
    def cleanUp(): Unit = {
      in.close()
      out.close()
      clientSocket.close()
      serverSocket.close()
    }
  }

  private def http(): Unit = {
    //Setup
    Static.info("ModelarDB: preparing HTTP end-point (Port: 9999)")
    val server = HttpServer.create(new java.net.InetSocketAddress(9999), 0)

    //Query
    class QueryHandler extends HttpHandler {
      override def handle(httpExchange: HttpExchange): Unit = {
        val request = httpExchange.getRequestBody
        val reader = new java.io.BufferedReader(new java.io.InputStreamReader(request))

        httpExchange.sendResponseHeaders(200, 0)
        val response = httpExchange.getResponseBody
        execute(reader.readLine.trim(), response)
        response.close()
      }
    }

    //Starts a HTTP server that executes QueryHandler for each incoming request on /
    server.createContext("/", new QueryHandler())
    server.start()
    Static.info("ModelarDB: the HTTP server is ready to receive queries (Port: 9999)")
    scala.io.StdIn.readLine() //Prevents the method from returning to keep the server running

    //Cleanup
    server.stop(0)
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
      val query_rewritten = query.replace("COUNT_S(#)", "COUNT_S(start_time, end_time, sampling_interval)")
          .replace("#", "tid, start_time, end_time, sampling_interval, mtid, model, gaps")
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
