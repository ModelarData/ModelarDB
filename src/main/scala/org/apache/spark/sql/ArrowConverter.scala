package org.apache.spark.sql

import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.util.ArrowUtils

object ArrowConverter {

  def toArrow(df: DataFrame): Schema = {
    ArrowUtils.toArrowSchema(df.schema, "UTC")
  }

}
