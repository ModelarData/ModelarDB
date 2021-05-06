package dk.aau.modelardb.engines.h2

import dk.aau.modelardb.core.models.{ModelType, Segment}
import dk.aau.modelardb.core.utility.CubeFunction

import java.sql.{Connection, Timestamp}
import org.h2.api.AggregateFunction

import java.util.Calendar
import scala.collection.mutable

/* Documentation:
 * http://www.h2database.com/javadoc/org/h2/api/Aggregate.html
 * http://www.h2database.com/javadoc/org/h2/api/AggregateFunction.html */
//Simple Aggregates
//Count
class CountS extends AggregateFunction {

  /** Public Methods **/
  override def init(conn: Connection): Unit = {
  }

  override def getType(inputTypes: Array[Int]): Int = {
    java.sql.Types.BIGINT
  }

  override def add(row: Any): Unit = {
    val values = row.asInstanceOf[Array[Object]]
    val st = values(0).asInstanceOf[java.sql.Timestamp]
    val et = values(1).asInstanceOf[java.sql.Timestamp]
    val res = values(2).asInstanceOf[java.lang.Integer]
    this.count = this.count + ((et.getTime - st.getTime) / res) + 1
  }

  override def getResult: AnyRef = {
    this.count.asInstanceOf[AnyRef]
  }

  /** Instance Variables **/
  private var count: Long = 0
}

//Min
class MinS extends AggregateFunction {

  /** Public Methods **/
  override def init(conn: Connection): Unit = {
    this.mtc = H2.h2storage.modelTypeCache
    this.sfc = H2.h2storage.timeSeriesScalingFactorCache
  }

  override def getType(inputTypes: Array[Int]): Int = {
    java.sql.Types.FLOAT
  }

  override def add(row: Any): Unit = {
    val segment = rowToSegment(row)
    this.min = Math.min(this.min, segment.min() / this.sfc(segment.tid))
  }

  override def getResult: AnyRef = {
    if (this.min == Float.PositiveInfinity) {
      null
    } else {
      this.min.asInstanceOf[AnyRef]
    }
  }

  def rowToSegment(row: Any): Segment = {
    val values = row.asInstanceOf[Array[Object]]
    val model = this.mtc(values(4).asInstanceOf[Int])
    model.get(
      values(0).asInstanceOf[Int], values(1).asInstanceOf[Timestamp].getTime, values(2).asInstanceOf[Timestamp].getTime,
      values(3).asInstanceOf[Int], values(5).asInstanceOf[Array[Byte]], values(6).asInstanceOf[Array[Byte]])
  }

  /** Instance Variables **/
  private var min: Float = Float.PositiveInfinity
  private var mtc: Array[ModelType] = _
  private var sfc: Array[Float] = _
}

//Max
class MaxS extends AggregateFunction {

  /** Public Methods **/
  override def init(conn: Connection): Unit = {
    this.mtc = H2.h2storage.modelTypeCache
    this.sfc = H2.h2storage.timeSeriesScalingFactorCache
  }

  override def getType(inputTypes: Array[Int]): Int = {
    java.sql.Types.FLOAT
  }

  override def add(row: Any): Unit = {
    val segment = rowToSegment(row)
    this.max = Math.max(this.max, segment.max() / this.sfc(segment.tid))
  }

  override def getResult: AnyRef = {
    if (this.max == Float.NegativeInfinity) {
      null
    } else {
      this.max.asInstanceOf[AnyRef]
    }
  }

  def rowToSegment(row: Any): Segment = {
    val values = row.asInstanceOf[Array[Object]]
    val model = this.mtc(values(4).asInstanceOf[Int])
    model.get(
      values(0).asInstanceOf[Int], values(1).asInstanceOf[Timestamp].getTime, values(2).asInstanceOf[Timestamp].getTime,
      values(3).asInstanceOf[Int], values(5).asInstanceOf[Array[Byte]], values(6).asInstanceOf[Array[Byte]])
  }

  /** Instance Variables **/
  private var max: Float = Float.NegativeInfinity
  private var mtc: Array[ModelType] = _
  private var sfc: Array[Float] = _
}

//Sum
class SumS extends AggregateFunction {

  /** Public Methods **/
  override def init(conn: Connection): Unit = {
    this.mtc = H2.h2storage.modelTypeCache
    this.sfc = H2.h2storage.timeSeriesScalingFactorCache
  }

  override def getType(inputTypes: Array[Int]): Int = {
    java.sql.Types.FLOAT
  }

  override def add(row: Any): Unit = {
    val segment = rowToSegment(row)
    this.sum += rowToSegment(row).sum() / this.sfc(segment.tid)
    this.added = true
  }

  override def getResult: AnyRef = {
    if (this.added) {
      this.sum.toFloat.asInstanceOf[AnyRef]
    } else {
      null
    }
  }

  def rowToSegment(row: Any): Segment = {
    val values = row.asInstanceOf[Array[Object]]
    val model = this.mtc(values(4).asInstanceOf[Int])
    model.get(
      values(0).asInstanceOf[Int], values(1).asInstanceOf[Timestamp].getTime, values(2).asInstanceOf[Timestamp].getTime,
      values(3).asInstanceOf[Int], values(5).asInstanceOf[Array[Byte]], values(6).asInstanceOf[Array[Byte]])
  }

  /** Instance Variables **/
  private var sum: Double = 0.0
  private var added = false
  private var mtc: Array[ModelType] = _
  private var sfc: Array[Float] = _
}

//Avg
class AvgS extends AggregateFunction {

  /** Public Methods **/
  override def init(conn: Connection): Unit = {
    this.mtc = H2.h2storage.modelTypeCache
    this.sfc = H2.h2storage.timeSeriesScalingFactorCache
  }

  override def getType(inputTypes: Array[Int]): Int = {
    java.sql.Types.FLOAT
  }

  override def add(row: Any): Unit = {
    val segment = rowToSegment(row)
    this.sum += segment.sum() / this.sfc(segment.tid)
    this.count += segment.length()
  }

  override def getResult: AnyRef = {
    if (this.count == 0) {
      null
    } else {
      (this.sum / this.count).toFloat.asInstanceOf[AnyRef]
    }
  }

  def rowToSegment(row: Any): Segment = {
    val values = row.asInstanceOf[Array[Object]]
    val model = this.mtc(values(4).asInstanceOf[Int])
    model.get(
      values(0).asInstanceOf[Int], values(1).asInstanceOf[Timestamp].getTime, values(2).asInstanceOf[Timestamp].getTime,
      values(3).asInstanceOf[Int], values(5).asInstanceOf[Array[Byte]], values(6).asInstanceOf[Array[Byte]])
  }

  /** Instance Variables **/
  private var sum: Double = 0.0
  private var count: Long = 0
  private var mtc: Array[ModelType] = _
  private var sfc: Array[Float] = _
}

//TODO: determine if a user-defined aggregate can return multiple rows?
//Time Aggregates
abstract class TimeAggregate(level: Int, bufferSize: Int, initialValue: Double) extends AggregateFunction {

  /** Public Methods **/
  override def init(conn: Connection): Unit = {
    this.mtc = H2.h2storage.modelTypeCache
    this.sfc = H2.h2storage.timeSeriesScalingFactorCache
  }

  override def getType(inputTypes: Array[Int]): Int = {
    java.sql.Types.JAVA_OBJECT
  }

  override def add(row: Any): Unit = {
    rowToSegment(row).cube(this.calendar, level, this.aggregate, this.current)
  }

  override def getResult: AnyRef = {
    val result = mutable.HashMap[Int, Double]()
    this.current.zipWithIndex.filter(_._1 != initialValue).foreach(t => {
      result(t._2) = t._1
    })

    if (result.isEmpty) {
      null
    } else {
      scala.collection.immutable.SortedMap[Int, Double]() ++ result
    }
  }

  def rowToSegment(row: Any): Segment = {
    val values = row.asInstanceOf[Array[Object]]
    val model = this.mtc(values(4).asInstanceOf[Int])
    model.get(
      values(0).asInstanceOf[Int], values(1).asInstanceOf[Timestamp].getTime, values(2).asInstanceOf[Timestamp].getTime,
      values(3).asInstanceOf[Int], values(5).asInstanceOf[Array[Byte]], values(6).asInstanceOf[Array[Byte]])
  }

  /** Instance Variables **/
  private val calendar = Calendar.getInstance()
  private var mtc: Array[ModelType] = _
  protected var sfc: Array[Float] = _
  protected val current: Array[Double] = Array.fill(bufferSize){initialValue}
  protected val aggregate: CubeFunction
}

//CountTime
class CountTime(level: Int, bufferSize: Int) extends TimeAggregate(level, bufferSize, 0.0) {

  /** Public Methods **/
  override def getResult: AnyRef = {
    val result = mutable.HashMap[Int, Long]()
    this.current.zipWithIndex.filter(_._1 != 0).foreach(t => {
      result(t._2) = t._1.longValue()
    })

    if (result.isEmpty) {
      null
    } else {
      scala.collection.immutable.SortedMap[Int, Long]() ++ result
    }
  }

  /** Instance Variables **/
  override protected val aggregate: CubeFunction = (segment: Segment, _: Int, field: Int, total: Array[Double]) => {
    total(field) = total(field) + segment.length.toDouble
  }
}
class CountMonth extends CountTime(2, 13)

//MinTime
class MinTime(level: Int, bufferSize: Int) extends TimeAggregate(level, bufferSize, Double.MaxValue) {
  override protected val aggregate: CubeFunction = (segment: Segment, _: Int, field: Int, total: Array[Double]) => {
    total(field) = Math.min(total(field), segment.min() / this.sfc(segment.tid))
  }
}
class MinMonth extends MinTime(2, 13)

//MaxTime
class MaxTime(level: Int, bufferSize: Int) extends TimeAggregate(level, bufferSize, Double.MinValue) {
  override protected val aggregate: CubeFunction = (segment: Segment, _: Int, field: Int, total: Array[Double]) => {
    total(field) = Math.max(total(field), segment.max() / this.sfc(segment.tid))
  }
}
class MaxMonth extends MaxTime(2, 13)

//SumTime
class SumTime(level: Int, bufferSize: Int) extends TimeAggregate(level, bufferSize, 0.0) {
  override protected val aggregate: CubeFunction = (segment: Segment, _: Int, field: Int, total: Array[Double]) => {
    total(field) = total(field) + (segment.sum() / this.sfc(segment.tid))
  }
}
class SumMonth extends SumTime(2, 13)

//AgTime
class AvgTime(level: Int, bufferSize: Int) extends TimeAggregate(level, 2 * bufferSize, 0.0) {

  /** Public Methods **/
  override def getResult: AnyRef = {
    val sums = this.current.length / 2
    val result = mutable.HashMap[Int, Double]()
    for (i <- 0 until sums) {
      val count = sums + i - 1
      if (this.current(count) != 0.0) {
        result(i) = this.current(i) / this.current(count)
      }
    }

    if (result.isEmpty) {
      null
    } else {
      scala.collection.immutable.SortedMap[Int, Double]() ++ result
    }
  }

  /** Instance Variables **/
  override protected val aggregate: CubeFunction = (segment: Segment, _: Int, field: Int, total: Array[Double]) => {
    //HACK: as field is continuous all of the counts are stored after the sum
    val count = bufferSize + field - 1
    total(field) = total(field) + (segment.sum / this.sfc(segment.tid))
    total(count) = total(count) + segment.length
  }
}
class AvgMonth extends AvgTime(2, 13)
