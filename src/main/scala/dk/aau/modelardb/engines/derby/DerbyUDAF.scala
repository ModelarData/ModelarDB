package dk.aau.modelardb.engines.derby

import java.sql.Blob
import java.io.{Externalizable, ObjectInput, ObjectOutput}

import org.apache.derby.agg.Aggregator

//TODO: should we create another view for the segment view or just assume that Derby is used for storage?
object Segment {

  /** Public Methods  **/
  def toSegment(gid: Int, start_time: Long, end_time: Long, mid: Int, params: Blob, gaps: Blob): Segment = {
    val paramsAsBytes = blobToByte(params)
    val gapsAsBytes = blobToByte(gaps)
    new Segment(gid, start_time, end_time, mid, paramsAsBytes, gapsAsBytes)
  }

  def blobToByte(blob: Blob): Array[Byte] = {
    val blobLength = blob.length.asInstanceOf[Int]
    val gapsAsBytes = if (blobLength == 0) {
      emptyArray
    } else {
      blob.getBytes(1, blobLength)
    }
    //blob.free() //Throws java.lang.UnsupportedOperationException: Not supported
    gapsAsBytes
  }

  /** Instance Variables **/
  val emptyArray = Array[Byte]()
}

class Segment(val gid: Int, val start_time: Long, val end_time: Long, val mid: Int, val params: Array[Byte], val gaps: Array[Byte]) extends Externalizable {

  /** Public Methods  **/
  override def writeExternal(out: ObjectOutput): Unit = { }
  override def readExternal(in: ObjectInput): Unit = { }
}

//Count
class COUNT_S extends Aggregator[Segment, Long, COUNT_S] {

  /** Public Methods  **/
  override def init(): Unit = {
    this.count = 0
  }

  override def accumulate(v: Segment): Unit = {
    val res = this.gmc(v.gid)(0)
    this.count = this.count + ((v.end_time - v.start_time) / res) + 1
  }

  override def merge(a: COUNT_S): Unit = {
    this.count = this.count + a.count
  }

  override def terminate(): Long = {
    this.count
  }

  def getCount(): Long = {
    this.count
  }

  /** Instance Variables **/
  var count: Long = 0
  val gmc = Derby.getStorage.getGroupMetadataCache
}