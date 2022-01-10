package dk.aau.modelardb.arrow

import org.apache.arrow.adapter.jdbc.{JdbcFieldInfo, JdbcToArrowConfig, JdbcToArrowConfigBuilder, JdbcToArrowUtils}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.{Field, FieldType, Schema}

import java.sql.JDBCType
import scala.collection.JavaConverters._


sealed trait Schemas {
  type DBSchema = Seq[(String, JDBCType)]

  val dbSchema: DBSchema
  val schemaMetadata: Map[String, String]

  def toArrow: Schema = new Schema(
    byName(dbSchema).map{ case (columnName, jdbcFieldInfo) =>
      val converter = jdbcToArrowConfig.getJdbcToArrowTypeConverter
      val arrowType = converter(jdbcFieldInfo)
      val fieldType = new FieldType(true, arrowType, /* dictionary encoding */ null, /* metadata */ null)
      new Field(columnName, fieldType, null) // Since children is null this does not work for nested schemas
    }.asJava,
    schemaMetadata.asJava
  )


  /* Creates a Map from column name to Arrow Type.
   * This is used to look up what Arrow type a given database column should be converted to. */
  def byName(dbSchema: DBSchema): Map[String, JdbcFieldInfo] =
    dbSchema.toMap.mapValues(v => new JdbcFieldInfo(v.getVendorTypeNumber))

  /* Creates a Map from column index to Arrow Type.
   * This is used to look up what Arrow type a given database column should be converted to. */
  def byValue(dbSchema: DBSchema): Map[Integer, JdbcFieldInfo] = dbSchema.zipWithIndex
    .map { case ((column, jdbcType), index) =>
      (index.asInstanceOf[Integer], new JdbcFieldInfo(jdbcType.getVendorTypeNumber))
    }.toMap

  lazy val jdbcToArrowConfig: JdbcToArrowConfig = new JdbcToArrowConfigBuilder()
    .setAllocator(new RootAllocator(Long.MaxValue))
    .setCalendar(JdbcToArrowUtils.getUtcCalendar)
    .setIncludeMetadata(false)
    .setTargetBatchSize(-1) // disables batching
    .setArraySubTypeByColumnIndexMap(byValue(dbSchema).asJava)
    .setArraySubTypeByColumnNameMap(byName(dbSchema).asJava)
    .build()

  def toSql(tableName: String, dbSchema: DBSchema) =
    s"CREATE TABLE IF NOT EXISTS $tableName ( ${dbSchema.map { case (column, dataType) => s"$column $dataType" }.mkString(", ")} )"

}

object SegmentSchema extends Schemas {

  override val schemaMetadata: Map[String, String] = Map("name" -> "segment")

  override val dbSchema: DBSchema = Seq(
    ("GID", JDBCType.INTEGER),
    ("START_TIME", JDBCType.BIGINT),
    ("END_TIME", JDBCType.BIGINT),
    ("MTID", JDBCType.INTEGER),
    ("MODEL", JDBCType.BINARY),
    ("GAPS", JDBCType.BINARY),
  )

  lazy val createTableSQL = toSql("segment", dbSchema)

}

object TimeseriesSchema extends Schemas {

  override val schemaMetadata: Map[String, String] = Map("name" -> "timeseries")

  override val dbSchema: DBSchema = Seq(
    ("TID", JDBCType.INTEGER),
    ("SCALING_FACTOR", JDBCType.REAL),
    ("SAMPLING_INTERVAL", JDBCType.INTEGER),
    ("GID", JDBCType.INTEGER),
  )

  lazy val createTableSQL = toSql("time_series", dbSchema)

}