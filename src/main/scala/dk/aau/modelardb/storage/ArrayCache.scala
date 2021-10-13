package dk.aau.modelardb.storage

class ArrayCache[T](val size: Int, offset: Int) {

  private val cache = new Array[T](size)

  def get(key: Int): T = {
    if ( (key - offset) < 0) throw new Exception("something is wrong with the offset")
    cache(key - offset)
  }

  def set(key: Int, value: T): Unit = {
    if ( (key - offset) < 0) throw new Exception("something is wrong with the offset")
    cache(key - offset) = value
  }

  def length: Int = size

  def array: Array[T] = cache.clone

}

