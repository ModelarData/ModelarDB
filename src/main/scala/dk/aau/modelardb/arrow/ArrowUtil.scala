package dk.aau.modelardb.arrow

import dk.aau.modelardb.core.SegmentGroup
import dk.aau.modelardb.core.models.Segment
import dk.aau.modelardb.engines.h2.H2Storage
import dk.aau.modelardb.storage.{CassandraStorage, JDBCStorage, ORCStorage, Storage}
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, IntVector, TimeStampVector, UInt4Vector, UInt8Vector, VarBinaryVector, VarCharVector, VectorSchemaRoot}
import org.apache.spark.sql.{ArrowConverter, DataFrame}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.functions.{col, from_utc_timestamp}

import java.nio.charset.StandardCharsets
import java.sql.ResultSet

object ArrowUtil {

  def storeData(storage: Storage, root: VectorSchemaRoot): Int = {
    val rowCount = root.getRowCount
    val groups = (0 until rowCount).map(index => toSegmentGroup(index, root))
    storage match {
      case storage: H2Storage => storage.storeSegmentGroups(groups.toArray, rowCount)
      case _ => throw new Exception("ArrowUtil got wrong storage type")
    }
    rowCount
  }

  def toSegmentGroup(index: Int, root: VectorSchemaRoot): SegmentGroup = {
    val gid = root.getVector("GID").asInstanceOf[IntVector].get(index)
    val start = root.getVector("START_TIME").asInstanceOf[BigIntVector].get(index)
    val end = root.getVector("END_TIME").asInstanceOf[BigIntVector].get(index)
    val mtid = root.getVector("MTID").asInstanceOf[IntVector].get(index)
    val model = root.getVector("MODEL").asInstanceOf[VarBinaryVector].get(index)
    val gaps = root.getVector("GAPS").asInstanceOf[VarBinaryVector].get(index)
    new SegmentGroup(gid, start, end, mtid, model, gaps)
  }

  def jdbcToArrow: ResultSet => VectorSchemaRoot = { rs =>
    val arrowJdbcConfig = TimeseriesSchema.jdbcToArrowConfig // config is the same for all Schemas
    val schema = JdbcToArrowUtils.jdbcToArrowSchema(rs.getMetaData, arrowJdbcConfig)
    val allocator = new RootAllocator(Long.MaxValue)
    val root = VectorSchemaRoot.create(schema, allocator)
    JdbcToArrowUtils.jdbcToArrowVectors(rs, root, arrowJdbcConfig)
    root
  }

  def dfToArrow: DataFrame => VectorSchemaRoot = { df =>
    val allocator = new RootAllocator(Long.MaxValue)
//    val schema = SegmentSchema.arrowSchema
    val schema = ArrowConverter.toArrow(df)
    val root = VectorSchemaRoot.create(schema, allocator)
    var index = 0
    val writer = ArrowWriter.create(root)

    val transformedDf = if (df.columns.contains("end_time")) {
      df.withColumn("end_time", from_utc_timestamp(col("end_time"), "UTC"))
    } else { df }

    transformedDf.collect().foreach { row =>
      val internalRow = InternalRow.fromSeq(row.toSeq)
      writer.write(internalRow)
      index += 1
    }
    writer.finish
    root
  }

  def addToRoot(index: Int, sg: SegmentGroup, root: VectorSchemaRoot): Unit = {
    root.getVector("GID").asInstanceOf[IntVector].setSafe(index, sg.gid)
    root.getVector("START_TIME").asInstanceOf[BigIntVector].setSafe(index, sg.startTime)
    root.getVector("END_TIME").asInstanceOf[BigIntVector].setSafe(index, sg.endTime)
    root.getVector("MTID").asInstanceOf[IntVector].setSafe(index, sg.mtid)
    root.getVector("MODEL").asInstanceOf[VarBinaryVector].setSafe(index, sg.model)
    root.getVector("GAPS").asInstanceOf[VarBinaryVector].setSafe(index, sg.offsets)
  }

  def mapToVector(index: Int, segment: Segment, schemaRoot: VectorSchemaRoot): VectorSchemaRoot = {
    schemaRoot.getVector("id").asInstanceOf[UInt8Vector].setSafe(index, segment.tid)
    schemaRoot.getVector("start_time").asInstanceOf[UInt8Vector].setSafe(index, segment.getStartTime)
    schemaRoot.getVector("end_time").asInstanceOf[UInt8Vector].setSafe(index, segment.getEndTime)
    schemaRoot.getVector("resolution").asInstanceOf[UInt4Vector].setSafe(index, segment.samplingInterval)
    schemaRoot
  }

  def mapToVector(index: Int, rs: ResultSet, schemaRoot: VectorSchemaRoot): VectorSchemaRoot = {
//    sid INT, start_time TIMESTAMP, end_time TIMESTAMP, resolution INT, mid INT, parameters BINARY, gaps BINARY
    schemaRoot.getVector("sid").asInstanceOf[UInt8Vector].setSafe(index, rs.getLong(1))
    schemaRoot.getVector("start_time").asInstanceOf[UInt8Vector].setSafe(index, rs.getTimestamp(2).getTime)
    schemaRoot.getVector("end_time").asInstanceOf[UInt8Vector].setSafe(index, rs.getTimestamp(3).getTime)
    schemaRoot.getVector("resolution").asInstanceOf[UInt4Vector].setSafe(index, rs.getInt(4))
    schemaRoot.getVector("mid").asInstanceOf[UInt4Vector].setSafe(index, rs.getInt(5))
    schemaRoot.getVector("parameters").asInstanceOf[VarBinaryVector].setSafe(index, rs.getBytes(6))
    schemaRoot.getVector("gaps").asInstanceOf[VarBinaryVector].setSafe(index, rs.getBytes(7))
    schemaRoot
  }

  def mapToVector(index: Int, rowString: String, schemaRoot: VectorSchemaRoot): VectorSchemaRoot = {
    schemaRoot.getVector("result").asInstanceOf[VarCharVector].setSafe(index, rowString.getBytes(StandardCharsets.UTF_8))
    schemaRoot
  }

  private[arrow] def insertTestData(n: Int, root: VectorSchemaRoot): VectorSchemaRoot = {
    val rng =  scala.util.Random
    (0 to n).foreach { index =>
      root.getVector("SID").asInstanceOf[BigIntVector]
        .setSafe(index, rng.nextInt(1000))

      root.getVector("START_TIME").asInstanceOf[TimeStampVector]
        .setSafe(index, rng.nextLong())

      root.getVector("END_TIME").asInstanceOf[TimeStampVector]
        .setSafe(index, rng.nextLong())

      root.getVector("RESOLUTION").asInstanceOf[IntVector]
        .setSafe(index, rng.nextInt(1000))

      root.getVector("MID").asInstanceOf[BigIntVector]
        .setSafe(index, rng.nextInt(1000))

      root.getVector("PARAMETERS").asInstanceOf[VarBinaryVector]
        .setSafe(index, rng.nextString(10).getBytes(StandardCharsets.UTF_8))

      root.getVector("GAPS").asInstanceOf[VarBinaryVector]
        .setSafe(index, rng.nextString(10).getBytes(StandardCharsets.UTF_8))
    }
    root.setRowCount(n)
    root
  }

  private[arrow] def insertTestSGData(n: Int, root: VectorSchemaRoot): VectorSchemaRoot = {
    val rng =  scala.util.Random
    (0 to n).foreach { index =>
      root.getVector("GID").asInstanceOf[IntVector]
        .setSafe(index, 888)
//        .setSafe(index, rng.nextInt(1000))

      root.getVector("START_TIME").asInstanceOf[BigIntVector]
        .setSafe(index, rng.nextLong())

      root.getVector("END_TIME").asInstanceOf[BigIntVector]
        .setSafe(index, rng.nextLong())

      root.getVector("MID").asInstanceOf[IntVector]
        .setSafe(index, rng.nextInt(1000))

      root.getVector("PARAMETERS").asInstanceOf[VarBinaryVector]
        .setSafe(index, rng.nextString(10).getBytes(StandardCharsets.UTF_8))

      root.getVector("GAPS").asInstanceOf[VarBinaryVector]
        .setSafe(index, rng.nextString(10).getBytes(StandardCharsets.UTF_8))
    }
    root.setRowCount(n)
    root
  }

}
