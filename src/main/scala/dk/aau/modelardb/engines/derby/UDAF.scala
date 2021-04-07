package dk.aau.modelardb.engines.derby

import dk.aau.modelardb.core.models.{Model, Segment}
import dk.aau.modelardb.core.utility.CubeFunction

import java.sql.Timestamp
import java.io.{Externalizable, ObjectInput, ObjectOutput}
import org.apache.derby.agg.Aggregator
import dk.aau.modelardb.engines.RDBMSEngineUtilities

import java.util.Calendar
import scala.collection.mutable

//Documentation: https://db.apache.org/derby/docs/10.15/ref/rrefsqljexternalname.html
object SegmentData {
  def apply(sid: Int, start_time: Timestamp , end_time: Timestamp, resolution: Int, mid: Int, params: Array[Byte], gaps: Array[Byte]): SegmentData = {
    new SegmentData(sid, start_time.getTime, end_time.getTime, resolution, mid, params, gaps)
  }
}

//Documentation: https://db.apache.org/derby/docs/10.15/devguide/cdevspecialudt.html
class SegmentData(val sid: Int, val start_time: Long, val end_time: Long, val resolution: Int, val mid: Int, val params: Array[Byte], val gaps: Array[Byte]) extends Externalizable {
  override def writeExternal(out: ObjectOutput): Unit = {}
  override def readExternal(in: ObjectInput): Unit = {}
  def decompress(mc: Array[Model]): Segment = {
    mc(this.mid).get(this.sid, this.start_time, this.end_time, this.resolution, this.params, this.gaps)
  }
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

//Simple Aggregates
//Count
class CountS extends Aggregator[SegmentData, Long, CountS] {

  /** Public Methods  **/
  override def init(): Unit = {
    this.gmc = RDBMSEngineUtilities.getStorage.groupMetadataCache
  }

  override def accumulate(v: SegmentData): Unit = {
    this.count = this.count + ((v.end_time - v.start_time) / this.gmc(v.sid)(0)) + 1
  }

  override def merge(a: CountS): Unit = {
    this.count = this.count + a.count
  }

  override def terminate(): Long = {
    this.count
  }

  /** Instance Variables **/
  private var count: Long = 0
  private var gmc: Array[Array[Int]] = _
}

//Min
class MinS extends Aggregator[SegmentData, Float, MinS] {

  /** Public Methods  **/
  override def init(): Unit = {
    this.mc = Derby.derbyStorage.modelCache
  }

  override def accumulate(v: SegmentData): Unit = {
    this.min = Math.min(this.min, v.decompress(mc).min())
  }

  override def merge(a: MinS): Unit = {
    this.min = Math.min(this.min, a.min)
  }

  override def terminate(): Float = {
    this.min
  }

  /** Instance Variables **/
  private var min: Float = Float.MaxValue
  private var mc: Array[Model] = _
}

//Max
class MaxS extends Aggregator[SegmentData, Float, MaxS] {

  /** Public Methods  **/
  override def init(): Unit = {
    this.mc = Derby.derbyStorage.modelCache
  }

  override def accumulate(v: SegmentData): Unit = {
    this.max = Math.max(this.max, v.decompress(this.mc).max())
  }

  override def merge(a: MaxS): Unit = {
    this.max = Math.max(this.max, a.max)
  }

  override def terminate(): Float = {
    this.max
  }

  /** Instance Variables **/
  private var max: Float = Float.MinValue
  private var mc: Array[Model] = _
}

//Sum
class SumS extends Aggregator[SegmentData, Float, SumS] {

  /** Public Methods  **/
  override def init(): Unit = {
    this.mc = Derby.derbyStorage.modelCache
  }

  override def accumulate(v: SegmentData): Unit = {
    this.sum = this.sum + v.decompress(this.mc).sum()
  }

  override def merge(a: SumS): Unit = {
    this.sum = this.sum + a.sum
  }

  override def terminate(): Float = {
    this.sum.toFloat
  }

  /** Instance Variables **/
  private var sum: Double = 0.0
  private var mc: Array[Model] = _
}

//Avg
class AvgS extends Aggregator[SegmentData, Float, AvgS] {

  /** Public Methods  **/
  override def init(): Unit = {
    this.mc = Derby.derbyStorage.modelCache
  }

  override def accumulate(v: SegmentData): Unit = {
    val segment = v.decompress(this.mc)
    this.sum = this.sum + segment.sum()
    this.count = this.count + segment.length()
  }

  override def merge(a: AvgS): Unit = {
    this.sum = this.sum + a.sum
    this.count = this.count + a.count
  }

  override def terminate(): Float = {
    (this.sum / this.count).toFloat
  }

  /** Instance Variables **/
  private var sum: Double = 0.0
  private var count: Long = 0
  private var mc: Array[Model] = _
}

//TODO: determine if a user-defined aggregate can return multiple rows?
//Time Aggregates
class CountMonth extends Aggregator[SegmentData, Map[Int, Long], CountMonth] {

  /** Public Methods  **/
  override def init(): Unit = {
    this.mc = Derby.derbyStorage.modelCache
  }

  override def accumulate(v: SegmentData): Unit = {
    val segment = v.decompress(this.mc)
    segment.cube(this.calendar, 2, this.aggregate, this.current)
  }

  override def merge(a: CountMonth): Unit = {
    for (i <- this.current.indices){
      this.current(i) += a.current(i)
    }
  }

  override def terminate(): Map[Int, Long] = {
    val result = mutable.HashMap[Int, Long]()
    this.current.zipWithIndex.filter(_._1 != 0.0).foreach(t => {
      result(t._2) = t._1.toLong
    })
    //TODO: determine if multiple values can be returned https://db.apache.org/derby/docs/10.15/publishedapi/org.apache.derby.engine/org/apache/derby/agg/Aggregator.html#terminate()
    //  - Return map error:     java.sql.SQLSyntaxErrorException: User defined aggregate 'APP'.'COUNT_MONTH' is bound to external class 'dk.aau.modelardb.engines.derby.CountMonth'. The parameter types of that class could not be resolved.
    //  - Return array error: java.sql.SQLSyntaxErrorException: User defined aggregate 'APP'.'COUNT_MONTH' was declared to have this return Java type: 'class dk.aau.modelardb.engines.derby.SegmentData'. This does not extend the following actual bounding return Java type: 'class [J'.
    scala.collection.immutable.SortedMap[Int, Long]() ++ result
  }

  /** Instance Variables **/
  private var mc: Array[Model] = _
  private val calendar = Calendar.getInstance()
  //protected val current: Array[Double] = Array.fill(bufferSize){initialValue}
  protected val current: Array[Double] = Array.fill(13){0.0}
  //protected val aggregate: CubeFunction
 // override
  protected val aggregate: CubeFunction = (segment: Segment, _: Int, field: Int, total: Array[Double]) => {
    total(field) = total(field) + segment.length.toDouble
  }
}
