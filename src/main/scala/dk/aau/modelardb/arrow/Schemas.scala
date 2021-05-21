package dk.aau.modelardb.arrow

import dk.aau.modelardb.core.models.Segment
import org.apache.arrow.adapter.jdbc.{JdbcFieldInfo, JdbcToArrowConfig, JdbcToArrowConfigBuilder, JdbcToArrowUtils}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.{BigIntVector, FieldVector, ValueVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit, Types}

import java.sql.JDBCType
import scala.collection.JavaConverters._

/* With schemas I want to be able to
  specify the individual schemas and have have arrow schema and db schema generated automatically.
  I also want to be able to go from db type to the Arrow vector type.
  For this I need
  1) a way to go from db type to arrow type
  2) a way to find arrow vector from arrow type
 */

sealed trait Schemas {
  type DBSchema = Seq[(String, JDBCType)]

  val dbSchema: DBSchema

  def arrowSchema: Schema = new Schema(
    byName(dbSchema).map {case (columnName, jdbcFieldInfo) =>
      val converter = jdbcToArrowConfig.getJdbcToArrowTypeConverter
      val arrowType = converter(jdbcFieldInfo)
      val fieldType = new FieldType(true, arrowType, /* dictionary encoding */ null, /* metadata */ null)
      new Field(columnName, fieldType, null) // Since children is null this does not work for nested schemas
    }.asJava
  )

//  val vectorTypes = byName(dbSchema).mapValues{ jdbcFieldInfo =>
//    val converter = jdbcToArrowConfig.getJdbcToArrowTypeConverter
//    val arrowType = converter(jdbcFieldInfo)
//    val vectorType = Types.getMinorTypeForArrowType(arrowType)
//    vectorType match {
//      case MinorType.BIGINT =>
//    }
//  }

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

object SegmentGroupSchema extends Schemas {
  // gid INTEGER, start_time BIGINT, end_time BIGINT, mid INTEGER, params ${this.blobType}, gaps ${this.blobType}
  val dbSchema: DBSchema = Seq(
    ("GID", JDBCType.INTEGER),
    ("START_TIME", JDBCType.BIGINT),
    ("END_TIME", JDBCType.BIGINT),
    ("MID", JDBCType.INTEGER),
    ("PARAMETERS", JDBCType.BINARY),
    ("GAPS", JDBCType.BINARY),
  )

  lazy val createTableSQL = toSql("segment", dbSchema)

}

object SegmentSchema extends Schemas {
  // sid INT, start_time TIMESTAMP, end_time TIMESTAMP, resolution INT, mid INT, parameters BINARY, gaps BINARY ${H2.getDimensionColumns(dimensions)}
  val dbSchema: DBSchema = Seq(
    ("SID", JDBCType.BIGINT),
    ("START_TIME", JDBCType.TIMESTAMP),
    ("END_TIME", JDBCType.TIMESTAMP),
    ("RESOLUTION", JDBCType.INTEGER),
    ("MID", JDBCType.BIGINT),
    ("PARAMETERS", JDBCType.BINARY),
    ("GAPS", JDBCType.BINARY),
  )

  lazy val createTableSQL = toSql("segment", dbSchema)

}

object TypeConverter {

  /* Docs:
  https://www.cis.upenn.edu/~bcpierce/courses/629/jdkdocs/guide/jdbc/getstart/mapping.doc.html
  http://www.h2database.com/html/datatypes.html
   */
  def jdbcToArrow(jdbcType: JDBCType): FieldType = {
    val arrowType = jdbcType match {
      case JDBCType.BIT => new ArrowType.FixedSizeBinary(1)
      case JDBCType.TINYINT =>
        // TINYINT represents an 8-bit unsigned integer
        new ArrowType.Int(8, false)
      case JDBCType.SMALLINT =>
        // SMALLINT represents a 16-bit signed integer
        new ArrowType.Int(16, true)
      case JDBCType.INTEGER =>
        // INTEGER represents a 32-bit signed integer
        new ArrowType.Int(32, true)
      case JDBCType.BIGINT =>
        // BIGINT represents a 64-bit signed integer
        new ArrowType.Int(64, true)
      case JDBCType.FLOAT | JDBCType.DOUBLE =>
        // FLOAT or DOUBLE represents a "double precision" floating point number which supports 15 digits of mantissa.
        new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
      case JDBCType.REAL =>
        // REAL represents a "single precision" floating point number which supports 7 digits of mantissa
        new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
      case JDBCType.NUMERIC | JDBCType.DECIMAL =>
//        new ArrowType.Decimal()
      case JDBCType.CHAR | JDBCType.VARCHAR =>
        new ArrowType.Utf8()
      case JDBCType.LONGVARCHAR =>
        new ArrowType.LargeUtf8()
      case JDBCType.DATE =>
        new ArrowType.Date(DateUnit.MILLISECOND)
      case JDBCType.TIME =>
        new ArrowType.Time(TimeUnit.MILLISECOND, 32)
      case JDBCType.TIMESTAMP =>
      case JDBCType.BINARY =>
      case JDBCType.VARBINARY =>
      case JDBCType.LONGVARBINARY =>
      case JDBCType.NULL =>
      case JDBCType.OTHER =>
      case JDBCType.JAVA_OBJECT =>
      case JDBCType.DISTINCT =>
      case JDBCType.STRUCT =>
      case JDBCType.ARRAY =>
      case JDBCType.BLOB =>
      case JDBCType.CLOB =>
      case JDBCType.REF =>
      case JDBCType.DATALINK =>
      case JDBCType.BOOLEAN =>
      case JDBCType.ROWID =>
      case JDBCType.NCHAR =>
      case JDBCType.NVARCHAR =>
      case JDBCType.LONGNVARCHAR =>
      case JDBCType.NCLOB =>
      case JDBCType.SQLXML =>
      case JDBCType.REF_CURSOR =>
      case JDBCType.TIME_WITH_TIMEZONE =>
      case JDBCType.TIMESTAMP_WITH_TIMEZONE =>
    }
    FieldType.nullable(new ArrowType.Bool())
  }

}
