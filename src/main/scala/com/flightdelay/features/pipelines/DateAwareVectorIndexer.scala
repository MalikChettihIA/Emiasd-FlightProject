package com.flightdelay.features.pipelines

import org.apache.spark.ml.feature.{VectorIndexer, VectorIndexerModel}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
 * Custom VectorIndexer that excludes date-derived features from categorical treatment.
 *
 * Date-derived features (with suffixes: _year, _month, _day, _dayofweek, _unix, _hour, _minute)
 * are ordinal/continuous and should NOT be treated as categorical.
 *
 * @param uid Unique identifier
 * @param featureNames Array of feature names to identify date-derived features
 */
class DateAwareVectorIndexer(
  override val uid: String,
  val featureNames: Array[String]
) extends VectorIndexer(uid) {

  def this(featureNames: Array[String]) = this(
    org.apache.spark.ml.util.Identifiable.randomUID("dateAwareVectorIndexer"),
    featureNames
  )

  /**
   * Suffixes that identify date-derived features
   */
  private val dateSuffixes = Set("_year", "_month", "_day", "_dayofweek", "_unix", "_hour", "_minute")

  /**
   * Identify indices of date-derived features
   */
  private def getDateFeatureIndices: Set[Int] = {
    featureNames.zipWithIndex.collect {
      case (name, idx) if dateSuffixes.exists(suffix => name.endsWith(suffix)) => idx
    }.toSet
  }

  override def fit(dataset: org.apache.spark.sql.Dataset[_]): VectorIndexerModel = {
    val baseModel = super.fit(dataset)

    // Get indices of date features to exclude
    val dateIndices = getDateFeatureIndices

    if (dateIndices.nonEmpty) {
      // Filter out date features from category maps
      // Use .filter instead of .filterKeys to create a serializable Map
      val filteredCategoryMaps = baseModel.categoryMaps.filter { case (idx, _) => !dateIndices.contains(idx) }

      // Create new model with filtered category maps using reflection
      // (VectorIndexerModel doesn't expose a constructor with categoryMaps)
      val constructor = classOf[VectorIndexerModel].getDeclaredConstructor(
        classOf[String],
        classOf[Int],
        classOf[Map[Int, Map[Double, Int]]]
      )
      constructor.setAccessible(true)

      val newModel = constructor.newInstance(
        baseModel.uid,
        baseModel.numFeatures.asInstanceOf[Object],
        filteredCategoryMaps
      )

      // Copy parameters
      newModel.setInputCol(baseModel.getInputCol)
      newModel.setOutputCol(baseModel.getOutputCol)

      // Log excluded features
      println(s"[DateAwareVectorIndexer] Excluded ${dateIndices.size} date-derived features from categorical treatment:")
      dateIndices.toSeq.sorted.foreach { idx =>
        if (idx < featureNames.length) {
          println(s"  [$idx] ${featureNames(idx)}")
        }
      }

      newModel
    } else {
      baseModel
    }
  }
}

object DateAwareVectorIndexer {
  def apply(featureNames: Array[String]): DateAwareVectorIndexer = {
    new DateAwareVectorIndexer(featureNames)
  }
}
