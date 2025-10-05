package com.flightdelay.data.preprocessing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

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
  )(implicit spark: SparkSession): DataFrame = {

    println("\n" + "=" * 80)
    println("[Data Balancing] Random Under-Sampling - Start")
    println("[Data Balancing] Random Under-Sampling - Start")
    println("=" * 80)

    // Validate label column exists
    if (!df.columns.contains(labelColumn)) {
      throw new IllegalArgumentException(
        s"Label column '$labelColumn' not found in DataFrame. " +
        s"Available columns: ${df.columns.mkString(", ")}"
      )
    }

    // Count original distribution
    val totalCount = df.count()
    val delayedCount = df.filter(col(labelColumn) === 1.0).count()
    val onTimeCount = df.filter(col(labelColumn) === 0.0).count()

    val delayedRatio = (delayedCount.toDouble / totalCount) * 100
    val onTimeRatio = (onTimeCount.toDouble / totalCount) * 100

    println(s"\nOriginal dataset distribution:")
    println(f"  - Total samples:    $totalCount%,10d")
    println(f"  - Delayed flights:  $delayedCount%,10d ($delayedRatio%5.2f%%)")
    println(f"  - On-time flights:  $onTimeCount%,10d ($onTimeRatio%5.2f%%)")

    // Check if already balanced
    if (delayedCount == onTimeCount) {
      println("\n- Dataset is already balanced (50/50)")
      println("=" * 80 + "\n")
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

    println(s"\nBalancing strategy:")
    println(s"  - Minority class: $minorityLabel ($minorityCount samples)")
    println(s"  - Majority class: $majorityLabel ($majorityCount samples)")
    println(s"  - Random under-sampling majority class to $minorityCount samples")

    // Split data by label
    val minorityData = df.filter(col(labelColumn) === minorityValue).cache()
    val majorityData = df.filter(col(labelColumn) === majorityValue).cache()

    // Force cache materialization
    minorityData.count()
    majorityData.count()

    // Calculate sampling fraction for majority class
    val samplingFraction = minorityCount.toDouble / majorityCount

    println(s"\nApplying random under-sampling:")
    println(f"  - Sampling fraction: $samplingFraction%.6f")
    println(s"  - Random seed: $seed")

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

    println(s"\nBalanced dataset distribution:")
    println(f"  - Total samples:    $balancedCount%,10d")
    println(f"  - Delayed flights:  $balancedDelayedCount%,10d ($balancedDelayedRatio%5.2f%%)")
    println(f"  - On-time flights:  $balancedOnTimeCount%,10d ($balancedOnTimeRatio%5.2f%%)")
    println(f"  - Reduction ratio:  ${(balancedCount.toDouble / totalCount) * 100}%5.2f%%")
    println(f"  - Samples removed:  ${totalCount - balancedCount}%,10d")

    // Clean up cache
    minorityData.unpersist()
    majorityData.unpersist()
    sampledMajority.unpersist()

    println("=" * 80 + "\n")
    println("[Data Balancing] Random Under-Sampling - End")
    println("=" * 80 + "\n")

    balancedData
  }

  /**
   * Balance data with train/test split (TIST paper approach)
   *
   * This method implements the exact approach from the TIST paper:
   * 1. Split delayed flights 3:1 (train/test)
   * 2. Add on-time flights randomly to each split until balanced (50/50)
   *
   * @param df Input DataFrame with label columns
   * @param labelColumn Label column to balance on
   * @param trainRatio Train/test split ratio (default: 0.75 for 3:1)
   * @param seed Random seed for reproducibility
   * @param spark Implicit SparkSession
   * @return Tuple of (trainData, testData) both balanced
   */
  def balanceWithSplit(
    df: DataFrame,
    labelColumn: String = "label_is_delayed_15min",
    trainRatio: Double = 0.75,
    seed: Long = 42L
  )(implicit spark: SparkSession): (DataFrame, DataFrame) = {

    println("\n" + "=" * 80)
    println("[Data Balancing] TIST Random Under-Sampling with Train/Test Split")
    println("=" * 80)

    // Validate parameters
    require(trainRatio > 0.0 && trainRatio < 1.0,
      s"Train ratio must be between 0 and 1, got $trainRatio")

    if (!df.columns.contains(labelColumn)) {
      throw new IllegalArgumentException(
        s"Label column '$labelColumn' not found in DataFrame"
      )
    }

    // Split into delayed and on-time flights
    val delayedFlights = df.filter(col(labelColumn) === 1.0).cache()
    val onTimeFlights = df.filter(col(labelColumn) === 0.0).cache()

    val delayedCount = delayedFlights.count()
    val onTimeCount = onTimeFlights.count()

    println(s"\nOriginal distribution:")
    println(f"  - Delayed flights:  $delayedCount%,10d")
    println(f"  - On-time flights:  $onTimeCount%,10d")

    // Split delayed flights into train/test (e.g., 3:1)
    val Array(delayedTrain, delayedTest) = delayedFlights.randomSplit(
      Array(trainRatio, 1.0 - trainRatio),
      seed
    )

    val delayedTrainCount = delayedTrain.count()
    val delayedTestCount = delayedTest.count()

    println(s"\nDelayed flights split ($trainRatio train / ${1.0 - trainRatio} test):")
    println(f"  - Train: $delayedTrainCount%,10d")
    println(f"  - Test:  $delayedTestCount%,10d")

    // Sample on-time flights for train set (to match delayed count)
    val trainSamplingFraction = delayedTrainCount.toDouble / onTimeCount
    val onTimeTrain = onTimeFlights.sample(
      withReplacement = false,
      fraction = trainSamplingFraction,
      seed = seed
    ).cache()

    val onTimeTrainCount = onTimeTrain.count()

    // Sample on-time flights for test set (from remaining on-time flights)
    val remainingOnTime = onTimeFlights.except(onTimeTrain)
    val testSamplingFraction = delayedTestCount.toDouble / remainingOnTime.count()
    val onTimeTest = remainingOnTime.sample(
      withReplacement = false,
      fraction = testSamplingFraction,
      seed = seed + 1
    )

    val onTimeTestCount = onTimeTest.count()

    println(s"\nOn-time flights sampled:")
    println(f"  - Train: $onTimeTrainCount%,10d (to match $delayedTrainCount%,d delayed)")
    println(f"  - Test:  $onTimeTestCount%,10d (to match $delayedTestCount%,d delayed)")

    // Create balanced train and test sets
    val balancedTrain = delayedTrain.union(onTimeTrain)
    val balancedTest = delayedTest.union(onTimeTest)

    val trainTotal = balancedTrain.count()
    val testTotal = balancedTest.count()

    println(s"\nBalanced datasets:")
    println(f"  - Train: $trainTotal%,10d samples (${(delayedTrainCount.toDouble / trainTotal) * 100}%5.2f%% delayed)")
    println(f"  - Test:  $testTotal%,10d samples (${(delayedTestCount.toDouble / testTotal) * 100}%5.2f%% delayed)")

    // Clean up cache
    delayedFlights.unpersist()
    onTimeFlights.unpersist()
    onTimeTrain.unpersist()

    println("\n=" * 80)
    println("[Data Balancing] TIST Approach Complete")
    println("=" * 80 + "\n")

    (balancedTrain, balancedTest)
  }
}
