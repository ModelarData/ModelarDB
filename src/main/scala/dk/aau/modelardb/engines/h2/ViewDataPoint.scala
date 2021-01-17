package dk.aau.modelardb.engines.h2

import java.{lang, util}

import collection.JavaConverters._

import org.h2.api.TableEngine
import org.h2.command.ddl.CreateTableData
import org.h2.command.dml.AllColumnsForPlan
import org.h2.engine.{Database, DbObject, Session}
import org.h2.index.{Cursor, Index, IndexLookupBatch, IndexType}
import org.h2.result.{Row, SearchRow, SortOrder}
import org.h2.schema.Schema
import org.h2.store.Data
import org.h2.table.{Column, IndexColumn, Table, TableBase, TableFilter, TableType}
import org.h2.value.{Value, ValueFloat, ValueInt, ValueTimestamp}

import dk.aau.modelardb.core.DataPoint

class ViewDataPoint extends TableEngine {
  override def createTable(data: CreateTableData): TableBase = {
    new ViewDataPointTable(data)
  }
}

class ViewDataPointTable(data: CreateTableData) extends TableBase(data) {

  override def lock(session: Session, exclusive: Boolean, forceLockEvenInMvcc: Boolean): Boolean = {
    false
  }

  override def close(session: Session): Unit = {
    //Nothing to do
  }

  override def unlock(s: Session): Unit = ???

  override def addIndex(session: Session, indexName: String, indexId: Int, cols: Array[IndexColumn], indexType: IndexType, create: Boolean, indexComment: String): Index = ???

  override def removeRow(session: Session, row: Row): Unit = ???

  override def truncate(session: Session): Unit = ???

  override def addRow(session: Session, row: Row): Unit = ???

  override def checkSupportAlter(): Unit = ???

  override def getTableType: TableType = ???

  override def getScanIndex(session: Session): Index = {
    new ViewDataIndex(this)
  }

  override def getUniqueIndex: Index = ???

  override def getIndexes: util.ArrayList[Index] = {
    new util.ArrayList[Index]()
  }

  override def isLockedExclusively: Boolean = ???

  override def getMaxDataModificationId: Long = ???

  override def isDeterministic: Boolean = false

  override def canGetRowCount: Boolean = false

  override def canDrop: Boolean = ???

  override def getRowCount(session: Session): Long = ???

  override def getRowCountApproximation: Long = ???

  override def getDiskSpaceUsed: Long = ???

  override def checkRename(): Unit = ???
}

class ViewDataIndex(table: Table) extends Index {
  override def getPlanSQL: String = ???

  override def close(session: Session): Unit = ???

  override def add(session: Session, row: Row): Unit = ???

  override def remove(session: Session, row: Row): Unit = ???

  override def update(session: Session, oldRow: Row, newRow: Row): Unit = ???

  override def isFindUsingFullTableScan: Boolean = ???

  override def find(session: Session, first: SearchRow, last: SearchRow): Cursor = ???

  override def find(filter: TableFilter, first: SearchRow, last: SearchRow): Cursor = {
    new ViewDataCursor()
  }

  override def getCost(session: Session, masks: Array[Int], filters: Array[TableFilter], filter: Int, sortOrder: SortOrder, allColumnsSet: AllColumnsForPlan): Double = {
    1.0  //HACK: unclear what whe have to return...
  }

  override def remove(session: Session): Unit = ???

  override def truncate(session: Session): Unit = ???

  override def canGetFirstOrLast: Boolean = ???

  override def canFindNext: Boolean = ???

  override def findNext(session: Session, higherThan: SearchRow, last: SearchRow): Cursor = ???

  override def findFirstOrLast(session: Session, first: Boolean): Cursor = ???

  override def needRebuild(): Boolean = ???

  override def getRowCount(session: Session): Long = ???

  override def getRowCountApproximation: Long = ???

  override def getDiskSpaceUsed: Long = ???

  override def compareRows(rowData: SearchRow, compare: SearchRow): Int = ???

  override def getColumnIndex(col: Column): Int = {
    -1 //HACK: -1 seems to be mean no index for that column
  }

  override def isFirstColumn(column: Column): Boolean = ???

  override def getIndexColumns: Array[IndexColumn] = {
    Array[IndexColumn]()
  }

  override def getColumns: Array[Column] = ???

  override def getIndexType: IndexType = ???

  override def getTable: Table = {
    table
  }

  override def getRow(session: Session, key: Long): Row = ???

  override def isRowIdIndex: Boolean = ???

  override def canScan: Boolean = ???

  override def setSortedInsertMode(sortedInsertMode: Boolean): Unit = ???

  override def createLookupBatch(filters: Array[TableFilter], filter: Int): IndexLookupBatch = ???

  override def getSchema: Schema = ???

  override def isHidden: Boolean = ???

  override def getSQL(alwaysQuote: Boolean): String = ???

  override def getSQL(builder: lang.StringBuilder, alwaysQuote: Boolean): lang.StringBuilder = ???

  override def getChildren: util.ArrayList[DbObject] = ???

  override def getDatabase: Database = ???

  override def getId: Int = ???

  override def getName: String = ???

  override def getCreateSQLForCopy(table: Table, quotedName: String): String = ???

  override def getCreateSQL: String = ???

  override def getDropSQL: String = ???

  override def getType: Int = ???

  override def removeChildrenAndResources(session: Session): Unit = ???

  override def checkRename(): Unit = ???

  override def rename(newName: String): Unit = ???

  override def isTemporary: Boolean = ???

  override def setTemporary(temporary: Boolean): Unit = ???

  override def setComment(comment: String): Unit = ???

  override def getComment: String = ???
}

class ViewDataCursor() extends Cursor {
  override def get(): Row = ???

  override def getSearchRow: SearchRow = {
    val row = new ViewDataRow()
    row.setValue(0, ValueInt.get(this.dataPoint.sid))
    row.setValue(1, ValueTimestamp.fromMillis(this.dataPoint.timestamp, 0))
    row.setValue(2, ValueFloat.get(this.dataPoint.value))
    row
  }

  override def next(): Boolean = {
    if (this.dataPoints.hasNext) {
      this.dataPoint = this.dataPoints.next()
      true
    } else {
      false
    }
  }

  override def previous(): Boolean = false

  /** Instance Variables **/
  private val storage = H2.getStorage
  private val segments = storage.getSegments.iterator().asScala
  private val dataPoints = segments.flatMap(sg => sg.toSegments(storage))
    .flatMap(segment => segment.grid().iterator().asScala)
  private var dataPoint: DataPoint = null
}

class ViewDataRow() extends Row {
  override def getByteCount(dummy: Data): Int = ???

  override def isEmpty: Boolean = ???

  override def setDeleted(deleted: Boolean): Unit = ???

  override def isDeleted: Boolean = ???

  override def getValueList: Array[Value] = ???

  override def hasSharedData(other: Row): Boolean = ???

  override def getColumnCount: Int = ???

  override def getValue(index: Int): Value = {
    this.values(index)
  }

  override def setValue(index: Int, v: Value): Unit = {
    this.values(index) = v
  }

  override def setKey(old: SearchRow): Unit = ???

  override def setKey(key: Long): Unit = ???

  override def getKey: Long = ???

  override def getMemory: Int = ???

  /** Instance Variables **/
  val values = new Array[Value](3)
}
