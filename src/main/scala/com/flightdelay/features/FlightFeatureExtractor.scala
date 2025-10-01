package com.flightdelay.features

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import com.flightdelay.data.utils.DataQualityMetrics
import com.flightdelay.features.pipelines.BasicAutoPipeline
import org.apache.spark.ml.Pipeline

object FlightFeatureExtractor {

  private val maxCat = 32
  private val handleInvalid = "skip"

  private val _featuresVec = "featuresVec"
  private val _featuresVecIndex = "features"

  def extract(data: DataFrame, target: String): DataFrame = {

    //Supprimons tous les labels sauf la colonne target
    val labelsToDrop = data.columns
      .filter(colName => colName.startsWith("label_") && colName != target)
    val flightData = data.drop(labelsToDrop: _*)

    val filghtDataMetric = DataQualityMetrics.metrics(flightData)
    val textCols = filghtDataMetric.filter(col("colType").contains(DataQualityMetrics._text)).select("name").rdd.flatMap(x=>x.toSeq).map(x=>x.toString).collect
    val numericCols = filghtDataMetric.filter(col("colType").contains(DataQualityMetrics._numeric)).filter(!col("name").contains(target))
      .select("name").rdd.flatMap(x=>x.toSeq).map(x=>x.toString).collect

    val pipeline = new BasicAutoPipeline(textCols,numericCols,target,maxCat,handleInvalid)
    val extractedData = pipeline.fit(flightData)
    extractedData
  }

}
