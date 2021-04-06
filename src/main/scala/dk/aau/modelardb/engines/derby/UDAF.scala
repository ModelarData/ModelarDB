package dk.aau.modelardb.engines.derby

import java.sql.Timestamp
import java.io.{Externalizable, ObjectInput, ObjectOutput}
import org.apache.derby.agg.Aggregator
import dk.aau.modelardb.engines.RDBMSEngineUtilities

//Documentation: https://db.apache.org/derby/docs/10.15/ref/rrefsqljexternalname.html
object Segment {

  /** Public Methods  **/
  def toSegment(gid: Int, start_time: Timestamp , end_time: Timestamp, resolution: Int, mid: Int, params: Array[Byte], gaps: Array[Byte]): Segment = {
    new Segment(gid, start_time.getTime, end_time.getTime, resolution, mid, params, gaps)
  }
}

//Documentation: https://db.apache.org/derby/docs/10.15/devguide/cdevspecialudt.html
class Segment(val gid: Int, val start_time: Long, val end_time: Long, val resolution: Int, val mid: Int, val params: Array[Byte], val gaps: Array[Byte]) extends Externalizable {

  /** Public Methods  **/
  override def writeExternal(out: ObjectOutput): Unit = {}
  override def readExternal(in: ObjectInput): Unit = {}
}

//Documentation: https://db.apache.org/derby/docs/10.15/devguide/cdevspecialuda.html
//Count
class CountBig extends Aggregator[Int, Long, CountBig] {

  /** Public Methods  **/
  override def init(): Unit = {
  }

  override def accumulate(v: Int): Unit = {
    this.count += 1
  }

  override def merge(a: CountBig): Unit = {
    this.count = this.count + a.count
  }

  override def terminate(): Long = {
    this.count
  }

  /** Instance Variables **/
  var count: Long = 0
}

class CountS extends Aggregator[Segment, Long, CountS] {

  /** Public Methods  **/
  override def init(): Unit = {
    this.gmc = RDBMSEngineUtilities.getStorage.groupMetadataCache
  }

  override def accumulate(v: Segment): Unit = {
    val res = this.gmc(v.gid)(0)
    this.count = this.count + ((v.end_time - v.start_time) / res) + 1
  }

  override def merge(a: CountS): Unit = {
    this.count = this.count + a.count
  }

  override def terminate(): Long = {
    this.count
  }

  /** Instance Variables **/
  var count: Long = 0
  var gmc: Array[Array[Int]] = null
}