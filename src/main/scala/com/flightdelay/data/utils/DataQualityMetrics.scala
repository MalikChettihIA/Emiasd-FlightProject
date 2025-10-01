package com.flightdelay.data.utils

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col


object DataQualityMetrics {

  val _text    = "textual"
  val _numeric = "numeric"
  val _date    = "date"
  val _other   = "other"

  case class MetaData(name: String, origType: String, colType: String, compRatio: Float, nbDistinctValues: Long)

  //considers only three types: numeric, textual and other
  private def whichType(origType: String) = origType match {
    case "StringType" => _text
    case "IntegerType"|"DoubleType" => _numeric
    case "DateType"|"TimestampType" => _date
    case _ => _other
  }


  def metrics(data: DataFrame): DataFrame = {
    // imports nécessaires pour col(), toDS(), etc.
    import data.sparkSession.implicits._

    val totalCount: Long = data.count()

    val res: Seq[MetaData] = data.dtypes.toSeq.map {
      case (colName, colType) =>
        val nonNullCount: Long =
          data.filter(col(colName).isNotNull).count()

        val distinctCnt: Long =
          data.select(col(colName)).distinct().count()

        MetaData(
          name   = colName,
          origType    = colType,
          colType  = whichType(colType),
          compRatio = nonNullCount.toFloat / math.max(1L, totalCount).toFloat,
          nbDistinctValues = distinctCnt
        )
    }

    val metadata = res.toDS().toDF()
    metadata.persist()
    metadata.count()
    metadata
  }

  def SetMDColType(metaData: DataFrame, name: String, colType: String): DataFrame = {
    val metaData_updated = metaData.withColumn(
      "colType",
      when(col("name") === name, colType)
        .otherwise(col("colType"))
    )
    return metaData_updated
  }
}
