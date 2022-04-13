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

import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig
import org.apache.arrow.vector.VectorSchemaRoot

trait ArrowResultSet {
  def get(): VectorSchemaRoot
  def hasNext(): Boolean
  def fillNext(): Unit
  def close(): Unit

  //The size is significantly increased as each rows only consists of tid, ts, value, and members
  protected val DEFAULT_TARGET_BATCH_SIZE: Int = 1024 * JdbcToArrowConfig.DEFAULT_TARGET_BATCH_SIZE
}