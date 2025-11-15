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
   * @param delayThresholdMin Threshold (in minutes) for labeling delayed flights
   * @param filterOnDxEquals1 If true, only keep rows where dxCol == 1
   * @return DataFrame with a new column "is_delayed" (1 or 0)
   */
  def prepareLabeledDataset(
                             df: DataFrame,
                             dxCol: String,
                             delayThresholdMin: Int,
                             filterOnDxEquals1: Boolean = true
                           ): DataFrame = {
    validate(df, Seq("ARR_DELAY_NEW", dxCol))

    val base = if (filterOnDxEquals1) df.filter(col(dxCol) === 1) else df

    base.withColumn(
      "is_delayed",
      when(coalesce(col("ARR_DELAY_NEW").cast("double"), lit(0.0)) >= delayThresholdMin, 1)
        .otherwise(0)
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

      val delayed = labeledDf.filter(col("is_delayed") === 1)
      val onTime  = labeledDf.filter(col("is_delayed") === 0)

      // 1) Split des 'delayed' (narrow, pas de shuffle)
      val Array(trainDelayed, testDelayed) =
        delayed.randomSplit(Array(trainRatio, 1.0 - trainRatio), seed)

      val nTrainDelayed = trainDelayed.count()
      val nTestDelayed  = testDelayed.count()

      // 2) Tirage "on-time" disjoint sans except/orderBy :
      val nOnTime  = onTime.count()
      val needTrainOn = nTrainDelayed
      val needTestOn  = nTestDelayed

      val pTrain = math.min(1.0, needTrainOn.toDouble / math.max(1, nOnTime))
      val pTest  = math.min(1.0 - pTrain, needTestOn.toDouble / math.max(1, nOnTime))

      val onTimeWithR = onTime.withColumn("__r", rand(seed + 100)) // stable, reproductible
      val trainOnTimeApprox = onTimeWithR.filter(col("__r") < pTrain)
      val testOnTimeApprox  = onTimeWithR.filter(col("__r") >= pTrain && col("__r") < (pTrain + pTest))

      // 3) Ajustement EXACT (limit)
      val trainOnTime = trainOnTimeApprox.limit(needTrainOn.toInt).drop("__r")
      val testOnTime  = testOnTimeApprox .limit(needTestOn .toInt).drop("__r")

      // 4) Union et légère randomisation locale sans tri global
      def lightlyShuffle(df: DataFrame, seed: Long): DataFrame =
        df.withColumn("__k", (rand(seed)*10).cast("int"))
          .repartition(col("__k"))
          .sortWithinPartitions(col("__k"))
          .drop("__k")

      val devDataRaw  = lightlyShuffle(trainDelayed.unionByName(trainOnTime), seed + 200)
      val testDataRaw = lightlyShuffle(testDelayed .unionByName(testOnTime ), seed + 201)

      // 5) Logs de contrôle
      def logSplit(name: String, d: DataFrame): Unit = {
        val total = d.count()
        val nDel  = d.filter(col("is_delayed") === 1).count()
        val nOn   = d.filter(col("is_delayed") === 0).count()
        info(f"[$name] total=$total%8d | delayed=$nDel%8d | on-time=$nOn%8d | balanced=${nDel==nOn}")
      }
      logSplit("TRAIN (balanced)", devDataRaw)
      logSplit("TEST  (balanced)", testDataRaw)

      (devDataRaw, testDataRaw)
    }
  }
}