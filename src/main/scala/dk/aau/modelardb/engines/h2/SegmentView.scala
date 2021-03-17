package dk.aau.modelardb.engines.h2

import dk.aau.modelardb.core.models.Segment
import dk.aau.modelardb.engines.RDBMSEngineUtilities
import org.h2.api.TableEngine
import org.h2.command.ddl.CreateTableData
import org.h2.command.dml.AllColumnsForPlan
import org.h2.engine.{Database, DbObject, Session}
import org.h2.index.{Cursor, Index, IndexLookupBatch, IndexType}
import org.h2.result.{Row, SearchRow, SortOrder}
import org.h2.schema.Schema
import org.h2.store.Data
import org.h2.table._
import org.h2.value.{Value, ValueInt, ValueTimestamp}

import java.{lang, util}

class SegmentView extends TableEngine {
  override def createTable(data: CreateTableData): Table = new SegmentViewTable(data)
}

//noinspection NotImplementedCode
class SegmentViewTable(data: CreateTableData) extends TableBase(data) {

  override def getScanIndex(session: Session): Index = {
    new SegmentViewIndex(this)
  }

  override def getIndexes: util.ArrayList[Index] = new util.ArrayList[Index]()

  override def lock(session: Session, exclusive: Boolean, forceLockEvenInMvcc: Boolean): Boolean = false

  override def close(session: Session): Unit = { /* Nothing to do */ }

  override def isDeterministic: Boolean = false

  override def canGetRowCount: Boolean = false


  /* Unimplemented Methods */

  override def unlock(s: Session): Unit = ???

  override def addIndex(session: Session, indexName: String, indexId: Int, cols: Array[IndexColumn], indexType: IndexType, create: Boolean, indexComment: String): Index = ???

  override def removeRow(session: Session, row: Row): Unit = ???

  override def truncate(session: Session): Unit = ???

  override def addRow(session: Session, row: Row): Unit = ???

  override def checkSupportAlter(): Unit = ???

  override def getTableType: TableType = ???

  override def getUniqueIndex: Index = ???

  override def isLockedExclusively: Boolean = ???

  override def getMaxDataModificationId: Long = ???

  override def canDrop: Boolean = ???

  override def getRowCount(session: Session): Long = ???

  override def getRowCountApproximation: Long = ???

  override def getDiskSpaceUsed: Long = ???

  override def checkRename(): Unit = ???
}

//noinspection NotImplementedCode
class SegmentViewIndex(table: Table) extends Index {

  override def find(filter: TableFilter, first: SearchRow, last: SearchRow): Cursor = {
    new SegmentCursor(filter)
  }

  override def getColumnIndex(col: Column): Int = -1 //HACK: -1 seems to indicate that no index exists for that column

  override def getCost(
    session: Session,
    masks: Array[Int],
    filters: Array[TableFilter],
    filter: Int,
    sortOrder: SortOrder,
    allColumnsSet: AllColumnsForPlan): Double = {
    1.0  //HACK: unclear what whe have to return...
  }

  override def getTable: Table = table

  override def getIndexColumns: Array[IndexColumn] = Array[IndexColumn]()


  /* Unimplemented Methods */

  override def getPlanSQL: String = ???

  override def close(session: Session): Unit = ???

  override def add(session: Session, row: Row): Unit = ???

  override def remove(session: Session, row: Row): Unit = ???

  override def update(session: Session, oldRow: Row, newRow: Row): Unit = ???

  override def isFindUsingFullTableScan: Boolean = ???

  override def find(session: Session, first: SearchRow, last: SearchRow): Cursor = ???

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

  override def isFirstColumn(column: Column): Boolean = ???

  override def getColumns: Array[Column] = ???

  override def getIndexType: IndexType = ???

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

//noinspection NotImplementedCode
class SegmentCursor(filter: TableFilter) extends Cursor {
  val storage = RDBMSEngineUtilities.getStorage match {
    case h2 : H2Storage => h2
    case _ => throw new Exception("error")
  }
  var segments: Iterator[Segment] = storage
    .getSegmentGroups(filter)
    .flatMap(_.toSegments(RDBMSEngineUtilities.getStorage))

  var sg: Segment = null

  override def get(): Row = new SegmentRow(sg)

  override def getSearchRow: SearchRow = new SegmentRow(sg)

  override def next(): Boolean = {
    if (segments.hasNext) {
      sg = segments.next()
      true
    } else { false }
  }

  override def previous(): Boolean = false
}

//noinspection NotImplementedCode
class SegmentRow(segment: Segment) extends Row {

  override def getValue(index: Int): Value = {
    index match {
      case 0 => ValueInt.get(segment.sid)
      case 1 => ValueTimestamp.fromMillis(segment.getStartTime, 0)
      case 2 => ValueTimestamp.fromMillis(segment.getEndTime, 0)
      case 3 => ValueInt.get(segment.resolution)
    }
  }

  override def setValue(index: Int, v: Value): Unit = ???

  override def getColumnCount: Int = 4

  override def getByteCount(dummy: Data): Int = ???

  override def isEmpty: Boolean = ???

  override def setDeleted(deleted: Boolean): Unit = ???

  override def isDeleted: Boolean = ???

  override def getValueList: Array[Value] = ???

  override def hasSharedData(other: Row): Boolean = ???

  override def setKey(old: SearchRow): Unit = ???

  override def setKey(key: Long): Unit = ???

  override def getKey: Long = ???

  override def getMemory: Int = ???
}