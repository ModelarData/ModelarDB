package dk.aau.modelardb.engines

import akka.stream.scaladsl.SourceQueueWithComplete
import dk.aau.modelardb.core.SegmentGroup
import org.apache.arrow.vector.VectorSchemaRoot

trait QueryEngine {

  def start(): Unit

  def start(queue: SourceQueueWithComplete[SegmentGroup]): Unit

  def stop(): Unit

  def execute(query: String): VectorSchemaRoot

}
