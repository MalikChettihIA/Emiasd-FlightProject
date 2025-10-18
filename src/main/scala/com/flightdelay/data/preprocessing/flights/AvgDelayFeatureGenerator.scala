package com.flightdelay.data.preprocessing.flights

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object AvgDelayFeatureGenerator {

  // Delay thresholds (en minutes)
  private val DELAY_THRESHOLDS = Map(
    "15min" -> 15,
    "30min" -> 30,
    "45min" -> 45,
    "60min" -> 60,
    "90min" -> 90
  )

  /**
   * Version OPTIMISÉE pour éviter OOM
   */
  def enrichFlightsWithAvgDelay(
                                 flightData: DataFrame,
                                 sampleFraction: Option[Double] = None,
                                 enableCheckpoint: Boolean = true
                               )(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    println("")
    println("=" * 80)
    println("[AvgDelayFeatureGenerator] Flight Average Delay Generator - Start")
    println("=" * 80)

    // Monitoring mémoire initial
    logMemoryUsage("Before processing")

    // OPTIMISATION 1 : Sample si dataset trop gros
    val dataToProcess = sampleFraction match {
      case Some(fraction) =>
        println(s"\n⚠️  Sampling dataset with fraction: $fraction")
        val sampled = flightData.sample(withReplacement = false, fraction, seed = 42)
        val count = sampled.count()
        println(s"  Sampled to $count rows")
        sampled
      case None =>
        flightData
    }

    // Étape 1 : Préparation des timestamps
    println("\n[Step 1] Preparing timestamps...")

    val flightWithTimestamps = dataToProcess
      .withColumn("feature_utc_departure_timestamp",
        to_timestamp(
          concat(
            col("UTC_FL_DATE"),
            lit(" "),
            lpad(col("feature_utc_departure_hour_rounded"), 2, "0"),
            lit(":00:00")
          )
        )
      )
      .withColumn("ts_long", col("feature_utc_departure_timestamp").cast("long"))
      .repartition(200, col("ORIGIN_AIRPORT_ID"))

    // OPTIMISATION 2 : Checkpoint pour matérialiser
    val checkpointed = if (enableCheckpoint) {
      println("  ✓ Checkpointing intermediate data...")
      flightWithTimestamps.checkpoint()
    } else {
      flightWithTimestamps
    }

    val rowCount = checkpointed.count()
    println(s"  ✓ Processing $rowCount flights")

    logMemoryUsage("After checkpoint")

    // Étape 2 : Window spec optimisée
    println("\n[Step 2] Creating window specification...")

    val windowSpec = Window
      .partitionBy("ORIGIN_AIRPORT_ID")
      .orderBy("ts_long")
      .rangeBetween(-6 * 3600, -2 * 3600)

    // OPTIMISATION 3 : Calculer TOUTES les agrégations EN UNE SEULE PASSE
    println("\n[Step 3] Computing all statistics in single pass...")

    // Base statistics columns
    val baseStatsColumns = Seq(
      avg(col("label_arr_delay_filled")).over(windowSpec).alias("feature_avg_delay"),
      count(col("label_arr_delay_filled")).over(windowSpec).alias("feature_num_previous_flights"),
      stddev(col("label_arr_delay_filled")).over(windowSpec).alias("feature_stddev_delay"),
      max(col("label_arr_delay_filled")).over(windowSpec).alias("feature_max_delay"),
      min(col("label_arr_delay_filled")).over(windowSpec).alias("feature_min_delay")
    )

    // Proportion columns pour tous les seuils
    val proportionColumns = DELAY_THRESHOLDS.map { case (suffix, threshold) =>
      val labelCol = s"label_is_delayed_${suffix}"
      avg(col(labelCol).cast("double")).over(windowSpec).alias(s"feature_proportion_delayed_${suffix}")
    }.toSeq

    // Combiner toutes les colonnes
    val allNewColumns = baseStatsColumns ++ proportionColumns

    // Appliquer toutes les transformations en une seule fois
    val enrichedFlights = checkpointed.select(
      col("*") +: allNewColumns: _*
    )

    println(s"  ✓ Generated ${DELAY_THRESHOLDS.size} proportion features + 5 base statistics")

    // Étape 4 : Remplacer les null par 0
    println("\n[Step 4] Filling null values...")

    val proportionCols = DELAY_THRESHOLDS.keys.map(suffix => s"feature_proportion_delayed_${suffix}").toSeq
    val allStatsCols = Seq("feature_avg_delay", "feature_stddev_delay", "feature_max_delay", "feature_min_delay") ++ proportionCols

    val cleanedFlights = enrichedFlights
      .na.fill(0.0, allStatsCols)
      .na.fill(0, Seq("feature_num_previous_flights"))
      .drop("ts_long", "feature_utc_departure_timestamp")

    // OPTIMISATION 4 : Checkpoint final avant retour
    val finalResult = if (enableCheckpoint) {
      println("\n[Step 5] Checkpointing final result...")
      val result = cleanedFlights.checkpoint()
      result.count()
      result
    } else {
      cleanedFlights
    }

    // Cleanup
    checkpointed.unpersist()

    logMemoryUsage("After completion")

    println("\n[AvgDelayFeatureGenerator] Completed successfully")
    println("=" * 80)

    finalResult
  }

  /**
   * Version ULTRA-OPTIMISÉE avec Self-Join au lieu de Window
   * Pour datasets très larges (> 10M lignes)
   */
  def enrichFlightsWithAvgDelayAggregated(
                                           flightData: DataFrame
                                         )(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    println("\n[AvgDelayFeatureGenerator] Using AGGREGATED approach (for large datasets)")

    // Étape 1 : Préparer les timestamps
    val flightWithTimestamps = flightData
      .withColumn("utc_ts",
        to_timestamp(
          concat(
            col("UTC_FL_DATE"),
            lit(" "),
            lpad(col("feature_utc_departure_hour_rounded"), 2, "0"),
            lit(":00:00")
          )
        )
      )
      .withColumn("ts_long", col("utc_ts").cast("long"))
      .withColumn("ts_hour_bucket", (col("ts_long") / 3600).cast("long"))
      .checkpoint()

    println(s"  Processing ${flightWithTimestamps.count()} flights")

    // Étape 2 : Construire les colonnes d'agrégation
    val baseAggColumns = Seq(
      avg("label_arr_delay_filled").alias("bucket_avg_delay"),
      count("*").alias("bucket_flight_count"),
      stddev("label_arr_delay_filled").alias("bucket_stddev_delay"),
      max("label_arr_delay_filled").alias("bucket_max_delay"),
      min("label_arr_delay_filled").alias("bucket_min_delay")
    )

    val proportionAggColumns = DELAY_THRESHOLDS.map { case (suffix, _) =>
      avg(col(s"label_is_delayed_${suffix}").cast("double")).alias(s"bucket_prop_${suffix}")
    }.toSeq

    val allAggColumns = baseAggColumns ++ proportionAggColumns

    // Pré-agréger par aéroport et bucket d'heure
    val aggregatedStats = flightWithTimestamps
      .groupBy("ORIGIN_AIRPORT_ID", "ts_hour_bucket")
      .agg(allAggColumns.head, allAggColumns.tail: _*)
      .checkpoint()

    println(s"  Aggregated to ${aggregatedStats.count()} buckets")

    // Étape 3 : Window spec sur les buckets
    val windowSpec = Window
      .partitionBy("ORIGIN_AIRPORT_ID")
      .orderBy("ts_hour_bucket")
      .rowsBetween(-6, -2)

    // Colonnes de rolling stats
    val rollingStatsColumns = Seq(
      avg("bucket_avg_delay").over(windowSpec).alias("feature_avg_delay"),
      sum("bucket_flight_count").over(windowSpec).alias("feature_num_previous_flights"),
      avg("bucket_stddev_delay").over(windowSpec).alias("feature_stddev_delay"),
      max("bucket_max_delay").over(windowSpec).alias("feature_max_delay"),
      min("bucket_min_delay").over(windowSpec).alias("feature_min_delay")
    )

    val rollingProportionColumns = DELAY_THRESHOLDS.keys.map { suffix =>
      avg(col(s"bucket_prop_${suffix}")).over(windowSpec).alias(s"feature_proportion_delayed_${suffix}")
    }.toSeq

    val allRollingColumns = rollingStatsColumns ++ rollingProportionColumns

    val rollingStats = aggregatedStats.select(
      col("*") +: allRollingColumns: _*
    )

    // Étape 4 : Construire les colonnes pour la jointure
    val joinColumns = Seq(
      "ORIGIN_AIRPORT_ID",
      "ts_hour_bucket",
      "feature_avg_delay",
      "feature_num_previous_flights",
      "feature_stddev_delay",
      "feature_max_delay",
      "feature_min_delay"
    ) ++ DELAY_THRESHOLDS.keys.map(s => s"feature_proportion_delayed_$s")

    // Joindre avec le dataset original
    val enriched = flightWithTimestamps
      .join(
        rollingStats.select(joinColumns.head, joinColumns.tail: _*),
        Seq("ORIGIN_AIRPORT_ID", "ts_hour_bucket"),
        "left"
      )
      .drop("ts_long", "ts_hour_bucket", "utc_ts")
      .na.fill(0.0)
      .na.fill(0)

    println("  ✓ Enrichment completed")

    enriched
  }

  /**
   * Monitoring mémoire
   */
  private def logMemoryUsage(label: String): Unit = {
    val runtime = Runtime.getRuntime
    val maxMemory = runtime.maxMemory() / (1024 * 1024)
    val totalMemory = runtime.totalMemory() / (1024 * 1024)
    val freeMemory = runtime.freeMemory() / (1024 * 1024)
    val usedMemory = totalMemory - freeMemory
    val usagePercent = (usedMemory.toDouble / maxMemory * 100).toInt

    println(s"\n[$label] Memory:")
    println(f"  Used: ${usedMemory}%,d MB / ${maxMemory}%,d MB ($usagePercent%%)")

    if (usagePercent > 80) {
      println(s"  ⚠️  WARNING: Memory usage is ${usagePercent}% - Risk of OOM!")
      System.gc()
      Thread.sleep(1000)
    }
  }
}