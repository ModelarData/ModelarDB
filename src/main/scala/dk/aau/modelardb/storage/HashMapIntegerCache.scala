package dk.aau.modelardb.storage

import scala.collection.mutable

class HashMapIntegerCache[V](offset: Int) {

  private val cache = mutable.HashMap.empty[Int, V]

  def get(key: Int): V = cache(key+offset)
  def set(key: Int, value: V): Option[V] = cache.put(key+offset, value)

}
