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
    import data.sparkSession.implicits._

    val totalCount = data.count()

    // Construire les agrégations pour toutes les colonnes en une seule passe
    val aggregations = data.dtypes.flatMap { case (colName, colType) =>
      Seq(
        count(when(col(colName).isNotNull, 1)).alias(s"${colName}_nonNull"),
        countDistinct(col(colName)).alias(s"${colName}_distinct")
      )
    }

    // Une seule passe sur les données !
    val aggResult = data.agg(aggregations.head, aggregations.tail: _*)
    val row = aggResult.collect()(0)

    // Construire les métadonnées
    val res: Seq[MetaData] = data.dtypes.zipWithIndex.map { case ((colName, colType), idx) =>
      val nonNullCount = row.getLong(idx * 2)
      val distinctCnt = row.getLong(idx * 2 + 1)

      MetaData(
        name = colName,
        origType = colType,
        colType = whichType(colType),
        compRatio = nonNullCount.toFloat / math.max(1L, totalCount).toFloat,
        nbDistinctValues = distinctCnt
      )
    }

    res.toDS().toDF()
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
