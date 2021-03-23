package dk.aau.modelardb.engines.h2

import dk.aau.modelardb.core.models.Segment

import java.sql.{Connection, Timestamp}
import org.h2.api.AggregateFunction
import dk.aau.modelardb.engines.RDBMSEngineUtilities

//http://www.h2database.com/javadoc/org/h2/api/Aggregate.html
//http://www.h2database.com/javadoc/org/h2/api/AggregateFunction.html
//Count
class CountS extends AggregateFunction {

  /** Public Methods **/
  override def init(conn: Connection): Unit = {
    this.cache = RDBMSEngineUtilities.getStorage.groupMetadataCache
  }

  override def getType(inputTypes: Array[Int]): Int = {
    java.sql.Types.BIGINT
  }

  override def add(row: Any): Unit = {
    val values = row.asInstanceOf[Array[Object]]
    val gid = values(0).asInstanceOf[java.lang.Integer]
    val st = values(1).asInstanceOf[java.sql.Timestamp]
    val et = values(2).asInstanceOf[java.sql.Timestamp]
    val res = this.cache(gid)(0)
    this.count = this.count + ((et.getTime - st.getTime) / res) + 1
  }

  override def getResult: AnyRef = {
    this.count.asInstanceOf[AnyRef]
  }

  /** Instance Variables **/
  private var count: Long = 0
  private var cache: Array[Array[Int]] = null
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
    this.min = Math.min(this.min, UDAF.rowToSegment(row).min() )
  }

  override def getResult: AnyRef = {
    this.min.asInstanceOf[AnyRef]
  }

  /** Instance Variables **/
  private var min: Float = Float.MaxValue
}

object UDAF {
  /** Type Variables **/
  private val mc = H2.getH2Storage().modelCache

  /** Public Methods **/
  def rowToSegment(row: Any): Segment = {
    val values = row.asInstanceOf[Array[Object]]
    val model = mc(values(4).asInstanceOf[Int])
    //TODO: Fix deadlock occuring in FacebookGorillaModel if MinS is used but not for Min
    model.get(
      values(0).asInstanceOf[Int], values(1).asInstanceOf[Timestamp].getTime, values(2).asInstanceOf[Timestamp].getTime,
      values(3).asInstanceOf[Int], values(5).asInstanceOf[Array[Byte]], values(6).asInstanceOf[Array[Byte]])
  }
}