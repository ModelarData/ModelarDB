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
import java.net.{ServerSocket, InetSocketAddress}

import dk.aau.modelardb.core.Configuration
import dk.aau.modelardb.core.utility.Static

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.nio.file.{Files, Paths}
import java.util.concurrent.Executor
import scala.io.Source
import scala.collection.mutable
import scala.io.StdIn.readLine

object Interface {

  /** Public Methods **/
  def start(configuration: Configuration, sql: String => Array[String]): Unit = {
    if ( ! configuration.contains("modelardb.interface")) {
      return
    }

    val (interface, port) = this.getInterfaceAndPort(configuration)
    interface match {
      case "socket" => socket(configuration.getExecutorService, port, sql)
      case "http" => http(configuration.getExecutorService, port, sql)
      case "repl" => repl(configuration.getStorage, sql)
      case path if Files.exists(Paths.get(path)) => file(path, sql)
      case _ => throw new java.lang.UnsupportedOperationException("unknown value for modelardb.interface in the config file")
    }
  }

  /** Private Methods **/
  private def getInterfaceAndPort(configuration: Configuration): (String, Int) = {
    val interface = configuration.getString("modelardb.interface")
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
        //Query
        val in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream))
        val out = new PrintWriter(clientSocket.getOutputStream, true)

        try {
          while (true) {
            execute(in.readLine(), sql, out.write)
            out.flush()
          }
        } catch {
          case _: NullPointerException =>
            //Thrown if the client closes in while ModelarDB waits for input
        }

        out.close()
        in.close()
        clientSocket.close()
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

        //The query is executed with the result returned as an HTTP response
        val results = mutable.ArrayBuffer[String]()
        execute(reader.readLine.trim(), sql, line => results.append(line))
        val out = results.mkString("")
        httpExchange.sendResponseHeaders(200, out.length)
        val response = httpExchange.getResponseBody
        response.write(out.getBytes)
        response.close()
      }
    }

    //Configures the HTTP server to executes QueryHandler on a separate thread for each incoming request on /
    server.createContext("/", new QueryHandler())
    server.setExecutor(executor)
    server.start()
    Static.info(s"ModelarDB: HTTP end-point is ready (Port: $port)")
    readLine() //Prevents the method from returning to keep the server running

    //Cleanup
    server.stop(0)
    Static.info(s"ModelarDB: HTTP end-point is closed (Port: $port)")
  }

  private def repl(storage: String, sql: String => Array[String]): Unit = {
    val prompt = storage.substring(storage.lastIndexOf('/') + 1) + "> "
    do {
      print(prompt)
      execute(readLine, sql, print)
    } while(true)
  }

  private def file(path: String, sql: String => Array[String]): Unit = {
    //This method is only called if the file exist
    val st = System.currentTimeMillis()
    Static.info("ModelarDB: executing queries from " + path)
    val source = Source.fromFile(path)
    for (line: String <- source.getLines()) {
      execute(line, sql, print)
    }
    source.close()
    val et = System.currentTimeMillis() - st
    val jst = java.time.Duration.ofMillis(et)
    Static.info("ModelarDB: finished all queries after " + jst)
  }

  private def execute(queries: String,
    sql: String => Array[String], out: String => Unit): Unit = {
    //Executes each query if it is supported and not commented out
    for (queriesSplitPart <- queries.split(";")) {
      val query = queriesSplitPart.trim()
      if ( ! query.startsWith("--") && query.startsWith("SELECT")) {
        unsafeExecute(query, sql, out)
      }
    }
  }

  private def unsafeExecute(query: String,
    sql: String => Array[String], out: String => Unit): Unit = {
    //Executes a SQL query without performing any checking
    val st = System.currentTimeMillis()
    var result: Array[String] = null
    try {
      val query_rewritten =
        query.replace("COUNT_S(#)", "COUNT_S(tid, start_time, end_time)")
          .replace("#", "tid, start_time, end_time, mtid, model, gaps")
      result = sql(query_rewritten)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        result = Array(e.toString)
    }
    val et = System.currentTimeMillis() - st
    val jst = java.time.Duration.ofMillis(et)

    //Outputs the query result using the method provided as the arguments out
    out(s"""{\n  "time": "$jst",\n  "query": "$query",\n  "result":  [\n    """)
    if (result.nonEmpty) {
      var index = 0
      val end = result.length - 1
      while (index < end) {
        out(result(index))
        out(",\n    ")
        index += 1
      }
      out(result(index))
      out(s"""\n  ]\n}\n""")
    } else {
      out(s"""  ]\n}\n""")
    }
  }
}
