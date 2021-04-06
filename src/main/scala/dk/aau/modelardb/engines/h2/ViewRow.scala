package dk.aau.modelardb.engines.h2

import org.h2.result.{Row, SearchRow}
import org.h2.store.Data
import org.h2.value.Value

class ViewRow extends Row {

  /** Instance Variable **/
  private var values: Array[Value] = _

  /** Public Methods **/
  override def getByteCount(dummy: Data): Int = ???

  override def isEmpty: Boolean = ???

  override def setDeleted(deleted: Boolean): Unit = ???

  override def isDeleted: Boolean = ???

  def setValueList(values: Array[Value]): Unit = this.values = values

  override def getValueList: Array[Value] = ???

  override def hasSharedData(other: Row): Boolean = ???

  override def getColumnCount: Int = ???

  override def getValue(index: Int): Value = this.values(index)

  override def setValue(index: Int, v: Value): Unit = ???

  override def setKey(old: SearchRow): Unit = ???

  override def setKey(key: Long): Unit = ???

  override def getKey: Long = ???

  override def getMemory: Int = ???
}
