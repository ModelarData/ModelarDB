package dk.aau.modelardb.engines

import java.sql.{SQLException, Timestamp}
import java.util

object PredicatePushDown {

  /** Public Methods **/
  //Sid => Gid
  def sidPointToGidPoint(sid: Int, sgc: Array[Int]): Int = {
    if (sid < sgc.length) {
      sgc(sid)
    } else {
      -1
    }
  }

  def sidRangeToGidIn(startSid: Int, endSid: Int, sgc: Array[Int]): Array[Any] = {
    val maxSid = sgc.length
    if (endSid <= 0 || startSid >= maxSid) {
      //All sids are outside the range of assigned sids, so a sentinel is used to ensure no gids match
      return Array(-1)
    }

    //All sids within the range of assigned sids are translated with the set removing duplicates
    val gids = scala.collection.mutable.Set[Int]()
    for (sid <- Math.max(startSid, 1) to Math.min(endSid, maxSid)) {
      gids.add(sgc(sid))
    }
    gids.toArray
  }

  def sidInToGidIn(sids: Array[Any], sgc: Array[Int]): Array[Any] = {
    val maxSid = sgc.length

    //All sids in the IN clause are translated with the set removing duplicates
    val gids = scala.collection.mutable.Set[Int]()
    sids.foreach(obj => {
      val sid = obj.asInstanceOf[Int]
      gids.add(if (sid <= 0 || maxSid < sid) -1 else sgc(sid))
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
