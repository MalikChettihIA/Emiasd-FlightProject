package com.flightdelay.features.balancer

import com.flightdelay.config.AppConfiguration
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.flightdelay.utils.MetricsUtils
import com.flightdelay.utils.DebugUtils._

/**
 * Utility to build balanced train/test datasets (50/50 delayed vs on-time).
 *
 * Functionality is split into:
 *   1. prepareLabeledDataset → filtering & labeling
 *   2. buildBalancedTrainTest → balancing & splitting (returns Array(devDataRaw, testDataRaw))
 */
object DelayBalancedDatasetBuilder {

  /** Randomly select N rows without replacement. If N > count(df), returns all rows. */
  private def sampleN(df: DataFrame, n: Long, seed: Long): DataFrame = {
    val total = df.count()
    val take = math.min(n, total)
    if (take <= 0) df.limit(0)
    else df
      .withColumn("__rnd", rand(seed))
      .orderBy(col("__rnd"))
      .limit(take.toInt)
      .drop("__rnd")
  }

  /** Validate that required columns exist. */
  private def validate(df: DataFrame, cols: Seq[String]): Unit = {
    val missing = cols.filterNot(df.columns.contains)
    require(missing.isEmpty, s"Missing required columns: ${missing.mkString(", ")}")
  }

  /**
   * Step 1: Prepare dataset by filtering and labeling delayed flights.
   *
   * @param df                Original DataFrame (must contain ARR_DELAY_NEW and dxCol)
   * @param dxCol             Binary column (e.g., "D1", "D2_60", "D3", "D4")
   * @return DataFrame with a new column "is_delayed" (1 or 0)
   */
  def prepareLabeledDataset(
                             df: DataFrame,
                             dxCol: String
                           ): DataFrame = {
    validate(df, Seq(dxCol))

    df.withColumn(
      "is_delayed",
      when(col(dxCol) === 1, 1).otherwise(0)
    )
  }

  /**
   * Step 2: Build balanced train/test datasets from a labeled DataFrame.
   *
   * @param labeledDf  DataFrame that already contains "is_delayed"
   * @param trainRatio Ratio of delayed samples to use for train (e.g., 0.75 → 3:1 train/test)
   * @param seed       Random seed for reproducibility
   * @return Array(devDataRaw, testDataRaw) — both balanced (50/50)
   */
  def buildBalancedTrainTest(
                              labeledDf: DataFrame,
                              trainRatio: Double = 0.75,
                              seed: Long = 42L
                            )(implicit spark: SparkSession, configuration: AppConfiguration): (DataFrame, DataFrame) = {

    // 0) Sanity
    require(trainRatio > 0 && trainRatio < 1.0, s"trainRatio must be in (0,1), got $trainRatio")
    require(labeledDf.columns.contains("is_delayed"), "labeledDf must contain 'is_delayed'")

    MetricsUtils.withUiLabels(
      groupId = "DelayBalancedDatasetBuilder.buildBalancedTrainTest",
      desc    = "Create balanced Train/Test datasets (50/50 delayed vs on-time)",
      tags    = "sampling,split,balance"
    ) {

      validate(labeledDf, Seq("is_delayed"))

      val delayed = labeledDf.filter(col("is_delayed") === 1)
      val onTime  = labeledDf.filter(col("is_delayed") === 0)

      // Split delayed data into train/test
      val Array(trainDelayed, testDelayed) =
        delayed.randomSplit(Array(trainRatio, 1.0 - trainRatio), seed)

      val nTrainDelayed = trainDelayed.count()
      val nTestDelayed  = testDelayed.count()

      // Undersample on-time data to match delayed counts
      val trainOnTime = sampleN(onTime, nTrainDelayed, seed + 1)
      val testOnTime  = sampleN(onTime.except(trainOnTime), nTestDelayed, seed + 2)

      // Combine and shuffle
      val devDataRaw = trainDelayed.unionByName(trainOnTime)
        .withColumn("__rnd", rand(seed + 3))
        .orderBy(col("__rnd"))
        .drop("__rnd")

      val testDataRaw = testDelayed.unionByName(testOnTime)
        .withColumn("__rnd", rand(seed + 4))
        .orderBy(col("__rnd"))
        .drop("__rnd")

      // Quick logging
      whenDebug{
        def logSplit(name: String, d: DataFrame): Unit = {
          val total = d.count()
          val nDel  = d.filter(col("is_delayed") === 1).count()
          val nOn   = d.filter(col("is_delayed") === 0).count()
          info(f"[$name] total=$total%8d | delayed=$nDel%8d | on-time=$nOn%8d")
        }

        logSplit("DEV  (trainBalanced)", devDataRaw)
        logSplit("TEST (testBalanced)", testDataRaw)
      }
      (devDataRaw, testDataRaw)
    }
  }
}