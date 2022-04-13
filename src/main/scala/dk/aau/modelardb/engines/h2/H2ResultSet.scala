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
package dk.aau.modelardb.engines.h2

import dk.aau.modelardb.remote.ArrowResultSet

import org.apache.arrow.adapter.jdbc.{JdbcToArrowConfigBuilder, JdbcToArrowUtils}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot

import java.sql.DriverManager

class H2ResultSet(connectionString: String, query: String) extends ArrowResultSet {

  /** Public Methods **/
  def get(): VectorSchemaRoot = {
    this.vsr
  }

  def hasNext(): Boolean = {
    //As maybeHasNext is set by fillNext() an empty vsr will always be returned
    this.maybeHasNext
  }

  def fillNext(): Unit = {
    JdbcToArrowUtils.jdbcToArrowVectors(this.rs, this.vsr, this.arrowJdbcConfig)
    this.maybeHasNext = this.vsr.getRowCount > 0 //No rows where extracted
  }

  def close(): Unit = {
    this.rs.close()
    this.stmt.close()
    this.connection.close()
  }

  /** Instance Variables **/
  private val connection = DriverManager.getConnection(connectionString)
  private val stmt = this.connection.createStatement()
  private val rs = {
    this.stmt.execute(query)
    this.stmt.getResultSet
  }
  private val arrowJdbcConfig = new JdbcToArrowConfigBuilder()
    .setAllocator(new RootAllocator())
    .setTargetBatchSize(DEFAULT_TARGET_BATCH_SIZE)
    .build()
  private val vsr = {
    val schema = JdbcToArrowUtils.jdbcToArrowSchema(this.rs.getMetaData, this.arrowJdbcConfig)
    val vsr = VectorSchemaRoot.create(schema, new RootAllocator())
    vsr.setRowCount(DEFAULT_TARGET_BATCH_SIZE)
    vsr
  }
  private var maybeHasNext = true
}