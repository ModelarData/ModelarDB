package dk.aau.modelardb.engines.h2

import java.sql.Connection

import org.h2.api.AggregateFunction

import dk.aau.modelardb.engines.RDBMSEngineUtilities

//http://www.h2database.com/javadoc/org/h2/api/Aggregate.html
//http://www.h2database.com/javadoc/org/h2/api/AggregateFunction.html
class CountS extends AggregateFunction {

  /** Public Methods **/
  override def init(conn: Connection): Unit = {
    this.cache = RDBMSEngineUtilities.getStorage.getGroupMetadataCache
  }

  override def getType(inputTypes: Array[Int]): Int = {
    java.sql.Types.BIGINT
  }

  override def add(value: Any): Unit = {
    val values = value.asInstanceOf[Array[Object]]
    val gid = values(0).asInstanceOf[java.lang.Integer]
    val st = values(1).asInstanceOf[java.lang.Long]
    val et = values(2).asInstanceOf[java.lang.Long]
    val res = this.cache(gid)(0)
    this.count = this.count + ((et - st) / res) + 1
  }

  override def getResult: AnyRef = {
    count.asInstanceOf[AnyRef]
  }

  /** Instance Variables **/
  private var count: Long = 0
  private var cache: Array[Array[Int]] = null
}