package dk.aau.modelardb

import dk.aau.modelardb.core.SegmentGroup
import org.scalactic.Equality

import java.nio.charset.StandardCharsets
import java.time.Instant

object TestUtil {

  implicit val segmentGroupEq: Equality[SegmentGroup] =
    (a: SegmentGroup, b: Any) => b match {
      case sg: SegmentGroup =>
        a.mtid == sg.mtid &&
          a.gid == sg.gid &&
          a.startTime == sg.startTime &&
          a.endTime == sg.endTime &&
          a.model.sameElements(sg.model) &&
          a.offsets.sameElements(sg.offsets)
      case _ => false
    }

  def generateSegmentGroups(n: Int): List[SegmentGroup] = {
    val rng = scala.util.Random
    (1 to n).map { gid =>
      val start = Instant.now
      val end = start.plusSeconds(rng.nextInt(1000))
      val mid = 123
      val params = "params".getBytes(StandardCharsets.UTF_8)
      val gaps = "gaps".getBytes(StandardCharsets.UTF_8)
      new SegmentGroup(gid, start.toEpochMilli, end.toEpochMilli, mid, params, gaps)
    }.toList
  }

}
