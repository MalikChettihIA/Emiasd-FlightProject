package com.flightdelay.data.preprocessing.flights

import com.flightdelay.config.AppConfiguration
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.flightdelay.utils.DebugUtils._
import com.flightdelay.utils.MetricsUtils.withUiLabels

/**
 * Flight Data Balancer using Random Under-Sampling
 *
 * Implements the random under-sampling approach from the TIST paper to handle
 * class imbalance in flight delay prediction datasets.
 *
 * The algorithm:
 * 1. Splits data into delayed and on-time flights
 * 2. Randomly samples on-time flights to match delayed flights count
 * 3. Returns balanced dataset with 50/50 distribution
 *
 * Reference: TIST-Flight-Delay-final.pdf - Section on Data Balancing
 */
object FlightDataBalancer {

  /**
   * Balance the dataset using random under-sampling
   *
   * @param df Input DataFrame with label columns
   * @param labelColumn Label column to balance on (default: label_is_delayed_15min)
   * @param seed Random seed for reproducibility
   * @param spark Implicit SparkSession
   * @return Balanced DataFrame with 50/50 class distribution
   */
  def preprocess(
    df: DataFrame,
    labelColumn: String = "label_is_delayed_15min",
    seed: Long = 42L
  )(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    info("- Calling com.flightdelay.data.preprocessing.flights.FlightDataBalancer.preprocess")

    withUiLabels(
      groupId = "FlightDataBalancer.preprocess",
      desc = "Balance the dataset using random under-sampling",
      tags = "data-pipeline,flights,balanced,dataframe"
    ) {
      debug("=" * 80)
      debug("[Data Balancing] Random Under-Sampling - Start")
      debug("=" * 80)

      // Validate label column exists
      if (!df.columns.contains(labelColumn)) {
        throw new IllegalArgumentException(
          s"Label column '$labelColumn' not found in DataFrame. " +
          s"Available columns: ${df.columns.mkString(", ")}"
        )
      }

      // Count original distribution
      df.cache()

      val totalCount = df.count()
      val delayedCount = df.filter(col(labelColumn) === 1.0).count()
      val onTimeCount = df.filter(col(labelColumn) === 0.0).count()

      val delayedRatio = (delayedCount.toDouble / totalCount) * 100
      val onTimeRatio = (onTimeCount.toDouble / totalCount) * 100

      debug(s"Original dataset distribution:")
      debug(f"  - Total samples:    $totalCount%,10d")
      debug(f"  - Delayed flights:  $delayedCount%,10d ($delayedRatio%5.2f%%)")
      debug(f"  - On-time flights:  $onTimeCount%,10d ($onTimeRatio%5.2f%%)")

      // Check if already balanced
      if (delayedCount == onTimeCount) {
        debug("- Dataset is already balanced (50/50)")
        debug("=" * 80)
        return df
      }

      // Identify minority and majority classes
      val (minorityCount, majorityCount, minorityValue, majorityValue) =
        if (delayedCount < onTimeCount) {
          (delayedCount, onTimeCount, 1.0, 0.0)
        } else {
          (onTimeCount, delayedCount, 0.0, 1.0)
        }

      val minorityLabel = if (minorityValue == 1.0) "delayed" else "on-time"
      val majorityLabel = if (majorityValue == 1.0) "delayed" else "on-time"

      debug(s"Balancing strategy:")
      debug(s"  - Minority class: $minorityLabel ($minorityCount samples)")
      debug(s"  - Majority class: $majorityLabel ($majorityCount samples)")
      debug(s"  - Random under-sampling majority class to $minorityCount samples")

      // Split data by label
      val minorityData = df.filter(col(labelColumn) === minorityValue).cache()
      val majorityData = df.filter(col(labelColumn) === majorityValue).cache()

      // Force cache materialization
      minorityData.count()
      majorityData.count()

      // Calculate sampling fraction for majority class
      val samplingFraction = minorityCount.toDouble / majorityCount

      debug(s"Applying random under-sampling:")
      debug(f"  - Sampling fraction: $samplingFraction%.6f")
      debug(s"  - Random seed: $seed")

      // Randomly sample majority class
      val sampledMajority = majorityData.sample(
        withReplacement = false,
        fraction = samplingFraction,
        seed = seed
      ).cache()

      val sampledMajorityCount = sampledMajority.count()

      // Union balanced data
      val balancedData = minorityData.union(sampledMajority)

      val balancedCount = balancedData.count()
      val balancedDelayedCount = balancedData.filter(col(labelColumn) === 1.0).count()
      val balancedOnTimeCount = balancedData.filter(col(labelColumn) === 0.0).count()

      val balancedDelayedRatio = (balancedDelayedCount.toDouble / balancedCount) * 100
      val balancedOnTimeRatio = (balancedOnTimeCount.toDouble / balancedCount) * 100

      debug(s"Balanced dataset distribution:")
      debug(f"  - Total samples:    $balancedCount%,10d")
      debug(f"  - Delayed flights:  $balancedDelayedCount%,10d ($balancedDelayedRatio%5.2f%%)")
      debug(f"  - On-time flights:  $balancedOnTimeCount%,10d ($balancedOnTimeRatio%5.2f%%)")
      debug(f"  - Reduction ratio:  ${(balancedCount.toDouble / totalCount) * 100}%5.2f%%")
      debug(f"  - Samples removed:  ${totalCount - balancedCount}%,10d")

      // Clean up cache
      df.unpersist()
      minorityData.unpersist()
      majorityData.unpersist()
      sampledMajority.unpersist()

      debug("=" * 80)
      debug("[Data Balancing] Random Under-Sampling - End")
      debug("=" * 80)

      balancedData
    }

  }

}
