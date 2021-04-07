package dk.aau.modelardb.engines.h2

import dk.aau.modelardb.core.models.Segment
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
    val st = values(1).asInstanceOf[java.sql.Timestamp]
    val et = values(2).asInstanceOf[java.sql.Timestamp]
    val res = values(3).asInstanceOf[java.lang.Integer]
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
  }

  override def getType(inputTypes: Array[Int]): Int = {
    java.sql.Types.FLOAT
  }

  override def add(row: Any): Unit = {
    this.min = Math.min(this.min, UDAF.rowToSegment(row).min())
    this.updated = true
  }

  override def getResult: AnyRef = {
    if (updated) {
      this.min.asInstanceOf[AnyRef]
    } else {
      null
    }
  }

  /** Instance Variables **/
  private var min: Float = Float.MaxValue
  private var updated = false
}

//Max
class MaxS extends AggregateFunction {

  /** Public Methods **/
  override def init(conn: Connection): Unit = {
  }

  override def getType(inputTypes: Array[Int]): Int = {
    java.sql.Types.FLOAT
  }

  override def add(row: Any): Unit = {
    this.max = Math.max(this.max, UDAF.rowToSegment(row).max())
    this.updated = true
  }

  override def getResult: AnyRef = {
    if (updated) {
      this.max.asInstanceOf[AnyRef]
    } else {
      null
    }
  }

  /** Instance Variables **/
  private var max: Float = Float.MinValue
  private var updated = false
}

//Sum
class SumS extends AggregateFunction {

  /** Public Methods **/
  override def init(conn: Connection): Unit = {
  }

  override def getType(inputTypes: Array[Int]): Int = {
    java.sql.Types.FLOAT
  }

  override def add(row: Any): Unit = {
    this.sum += UDAF.rowToSegment(row).sum()
    this.updated = true
  }

  override def getResult: AnyRef = {
    if (updated) {
      this.sum.toFloat.asInstanceOf[AnyRef]
    } else {
      null
    }
  }

  /** Instance Variables **/
  private var sum: Double = 0.0
  private var updated = false
}

//Avg
class AvgS extends AggregateFunction {

  /** Public Methods **/
  override def init(conn: Connection): Unit = {
  }

  override def getType(inputTypes: Array[Int]): Int = {
    java.sql.Types.FLOAT
  }

  override def add(row: Any): Unit = {
    val segment = UDAF.rowToSegment(row)
    this.sum += segment.sum()
    this.count += segment.length()
    this.updated = true
  }

  override def getResult: AnyRef = {
    if (updated) {
      (this.sum / this.count).toFloat.asInstanceOf[AnyRef]
    } else {
      null
    }
  }

  /** Instance Variables **/
  private var sum: Double = 0.0
  private var count: Long = 0
  private var updated = false
}

//TODO: determine if a user-defined aggregate can return multiple rows?
//Time Aggregates
abstract class TimeAggregate(level: Int, bufferSize: Int, initialValue: Double) extends AggregateFunction {

  /** Public Methods **/
  override def init(conn: Connection): Unit = {
  }

  override def getType(inputTypes: Array[Int]): Int = {
    java.sql.Types.JAVA_OBJECT
  }

  override def add(row: Any): Unit = {
    val segment = UDAF.rowToSegment(row)
    segment.cube(this.calendar, level, this.aggregate, this.current)
  }

  override def getResult: AnyRef = {
    val result = mutable.HashMap[Int, Double]()
    this.current.zipWithIndex.filter(_._1 != initialValue).foreach(t => {
      result(t._2) = t._1
    })
    scala.collection.immutable.SortedMap[Int, Long]() ++ result
  }

  /** Instance Variables **/
  private val calendar = Calendar.getInstance()
  protected val current: Array[Double] = Array.fill(bufferSize){initialValue}
  protected val aggregate: CubeFunction
}

//CountTime
class CountTime(timeInterval: Int, bufferSize: Int) extends TimeAggregate(timeInterval, bufferSize, 0.0) {

  /** Public Methods **/
  override def getResult: AnyRef = {
    val result = mutable.HashMap[Int, Long]()
    this.current.zipWithIndex.filter(_._1 != 0).foreach(t => {
      result(t._2) = t._1.longValue()
    })
    scala.collection.immutable.SortedMap[Int, Long]() ++ result
  }

  /** Instance Variables **/
  override protected val aggregate: CubeFunction = (segment: Segment, _: Int, field: Int, total: Array[Double]) => {
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
  override def getResult: AnyRef = {
    val sums = this.current.length / 2
    val result = mutable.HashMap[Int, Double]()
    for (i <- 0 until sums) {
      val count = sums + i - 1
      if (this.current(count) != 0.0) {
        result(i) = this.current(i) / this.current(count)
      }
    }
    scala.collection.immutable.SortedMap[Int, Double]() ++ result
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

//UDFs
object UDAF {
  /** Type Variables **/
  private val mc = H2.h2Storage.modelCache

  /** Public Methods **/
  def rowToSegment(row: Any): Segment = {
    val values = row.asInstanceOf[Array[Object]]
    val model = mc(values(4).asInstanceOf[Int])
    model.get(
      values(0).asInstanceOf[Int], values(1).asInstanceOf[Timestamp].getTime, values(2).asInstanceOf[Timestamp].getTime,
      values(3).asInstanceOf[Int], values(5).asInstanceOf[Array[Byte]], values(6).asInstanceOf[Array[Byte]])
  }
}