package dk.aau.modelardb.engines

import java.sql.{SQLException, Timestamp}
import java.util

object PredicatePushDown {

  /** Public Methods **/
  //Tid => Gid
  def tidPointToGidPoint(tid: Int, tsgc: Array[Int]): Int = {
    if (tid < tsgc.length) {
      tsgc(tid)
    } else {
      -1
    }
  }

  def tidRangeToGidIn(startTid: Int, endTid: Int, tsgc: Array[Int]): Array[Any] = {
    val maxTid = tsgc.length
    if (endTid <= 0 || startTid >= maxTid) {
      //All tids are outside the range of assigned tids, so a sentinel is used to ensure no gids match
      return Array(-1)
    }

    //All tids within the range of assigned tids are translated with the set removing duplicates
    val gids = scala.collection.mutable.Set[Int]()
    for (tid <- Math.max(startTid, 1) to Math.min(endTid, maxTid)) {
      gids.add(tsgc(tid))
    }
    gids.toArray
  }

  def tidInToGidIn(tids: Array[Any], tsgc: Array[Int]): Array[Any] = {
    val maxTid = tsgc.length

    //All tids in the IN clause are translated with the set removing duplicates
    val gids = scala.collection.mutable.Set[Int]()
    tids.foreach(obj => {
      val tid = obj.asInstanceOf[Int]
      gids.add(if (tid <= 0 || maxTid < tid) -1 else tsgc(tid))
    })
    gids.toArray
  }

  //Dimensions => Gid
  def dimensionEqualToGidIn(column: String, value: Any, idc: util.HashMap[String, util.HashMap[Object, Array[Integer]]]): Array[Any] = {
    idc.get(column).getOrDefault(value, Array(Integer.valueOf(-1))).asInstanceOf[Array[Any]]
  }

  //Timestamp => BigInt
  def toLongThroughTimestamp(columnValue: AnyRef): Long = {
    columnValue match {
      case ts: Timestamp => ts.getTime
      case str: String => Timestamp.valueOf(str).getTime
      case cl => throw new SQLException(s"ModelarDB: a ${cl.getClass} cannot be converted to a Timestamp")
    }
  }
}
