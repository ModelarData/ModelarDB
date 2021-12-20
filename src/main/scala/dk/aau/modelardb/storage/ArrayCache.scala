package dk.aau.modelardb.storage

import scala.reflect.ClassTag

class ArrayCache[T: ClassTag](val size: Int, offset: Int) extends Serializable {

  private val cache = Array.ofDim[T](size)

  def get(key: Int): T = {
    val lookupKey = key - offset
    if ( lookupKey < 0) throw new Exception(s"Something is wrong with the offset: size=$size, offset=$offset, lookupKey=$lookupKey")
    cache(lookupKey)
  }

  def set(key: Int, value: T): Unit = {
    val lookupKey = key - offset
    if ( lookupKey < 0) throw new Exception(s"Something is wrong with the offset: size=$size, offset=$offset, lookupKey=$lookupKey")
    cache(lookupKey) = value
  }

  def length: Int = size

  def toArray: Array[T] = cache.clone

}

