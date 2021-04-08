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
//Segment Type
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
//Simple Aggregates
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
    this.mc = RDBMSEngineUtilities.getStorage.modelCache
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
    this.mc = RDBMSEngineUtilities.getStorage.modelCache
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
    this.mc = RDBMSEngineUtilities.getStorage.modelCache
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
    this.mc = RDBMSEngineUtilities.getStorage.modelCache
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
class DerbyMap(values: mutable.HashMap[Int, AnyVal]) {
  val result = scala.collection.immutable.SortedMap[Int, AnyVal]() ++ values
  override def toString(): String = this.result.toString()
}

abstract class TimeAggregate(level: Int, bufferSize: Int, initialValue: Double) extends Aggregator[SegmentData, DerbyMap, TimeAggregate] {

  /** Public Methods  **/
  override def init(): Unit = {
    this.mc = RDBMSEngineUtilities.getStorage.modelCache
  }

  override def accumulate(v: SegmentData): Unit = {
    v.decompress(this.mc).cube(this.calendar, level, this.aggregate, this.current)
  }

  override def merge(a: TimeAggregate): Unit = {
    for (i <- this.current.indices){
      this.current(i) += a.current(i)
    }
  }

  override def terminate(): DerbyMap = {
    val result = mutable.HashMap[Int, AnyVal]()
    this.current.zipWithIndex.filter(_._1 != 0.0).foreach(t => {
      result(t._2) = t._1
    })
    new DerbyMap(result)
  }

  /** Instance Variables **/
  private val calendar = Calendar.getInstance()
  private var mc: Array[Model] = _
  protected val current: Array[Double] = Array.fill(bufferSize){initialValue}
  protected val aggregate: CubeFunction
}

//CountTime
class CountTime(level: Int, bufferSize: Int) extends TimeAggregate(level, bufferSize, 0.0) {

  /** Public Methods **/
  override def terminate(): DerbyMap = {
    val result = mutable.HashMap[Int, AnyVal]()
    this.current.zipWithIndex.filter(_._1 != 0.0).foreach(t => {
      result(t._2) = t._1.toLong
    })
    new DerbyMap(result)
  }

  /** Instance Variables **/
  protected val aggregate: CubeFunction = (segment: Segment, _: Int, field: Int, total: Array[Double]) => {
    total(field) = total(field) + segment.length.toDouble
  }
}
class CountMonth extends CountTime(2, 13)

//MinTime
class MinTime(timeInterval: Int, bufferSize: Int) extends TimeAggregate(timeInterval, bufferSize, Double.MaxValue) {
  override protected val aggregate: CubeFunction = (segment: Segment, _: Int, field: Int, total: Array[Double]) => {
    total(field) = Math.min(total(field), segment.min())
  }
}
class MinMonth extends MinTime(2, 13)

//MaxTime
class MaxTime(timeInterval: Int, bufferSize: Int) extends TimeAggregate(timeInterval, bufferSize, Double.MinValue) {
  override protected val aggregate: CubeFunction = (segment: Segment, _: Int, field: Int, total: Array[Double]) => {
    total(field) = Math.max(total(field), segment.max())
  }
}
class MaxMonth extends MaxTime(2, 13)

//SumTime
class SumTime(timeInterval: Int, bufferSize: Int) extends TimeAggregate(timeInterval, bufferSize, 0.0) {
  override protected val aggregate: CubeFunction = (segment: Segment, _: Int, field: Int, total: Array[Double]) => {
    total(field) = total(field) + segment.sum()
  }
}
class SumMonth extends SumTime(2, 13)

//AgTime
class AvgTime(timeInterval: Int, bufferSize: Int) extends TimeAggregate(timeInterval, 2 * bufferSize, 0.0) {

  /** Public Methods **/
  override def terminate(): DerbyMap = {
    val sums = this.current.length / 2
    val result = mutable.HashMap[Int, AnyVal]()
    for (i <- 0 until sums) {
      val count = sums + i - 1
      if (this.current(count) != 0.0) {
        result(i) = this.current(i) / this.current(count)
      }
    }
    new DerbyMap(result)
  }

  /** Instance Variables **/
  override protected val aggregate: CubeFunction = (segment: Segment, _: Int, field: Int, total: Array[Double]) => {
    //HACK: as field is continuous all of the counts are stored after the sum
    val count = bufferSize + field - 1
    total(field) = total(field) + segment.sum
    total(count) = total(count) + segment.length
  }
}
class AvgMonth extends AvgTime(2, 13)