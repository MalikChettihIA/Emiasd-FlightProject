package com.flightdelay.data.preprocessing.flights

import com.flightdelay.config.{AppConfiguration, ExperimentConfig}
import com.flightdelay.data.preprocessing.weather.WeatherInteractionFeatures
import com.flightdelay.data.utils.TimeFeatureUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object FlightPreprocessingPipeline {

  /**
   * Exécute le pipeline complet de preprocessing
   * Charge les données depuis le fichier parquet généré par FlightDataLoader
   * @param configuration Configuration contenant les paramètres de chargement
   * @param spark Session Spark
   * @return DataFrame complètement préprocessé avec labels
   */
  def execute()(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    println("\n" + "=" * 80)
    println("[Preprocessing] Flight Data Preprocessing Pipeline - Start")
    println("=" * 80)

    val processedParquetPath = s"${configuration.common.output.basePath}/common/data/processed_flights.parquet"

    // Load raw data from parquet
    val rawParquetPath = s"${configuration.common.output.basePath}/common/data/raw_flights.parquet"
    println(s"\nLoading raw data from parquet:")
    println(s"  - Path: $rawParquetPath")
    val originalDf = spark.read.parquet(rawParquetPath)
    println(s"  - Loaded ${originalDf.count()} raw records")

    // Execute preprocessing pipeline (each step creates a new DataFrame)
    println("\n[Pipeline Step 1/9] Cleaning flight data...")
    val cleanedFlightData = FlightDataCleaner.preprocess(originalDf)

    println("[Pipeline Step 2/9] Enriching with WBAN...")
    val enrichedWithWBAN = FlightWBANEnricher.preprocess(cleanedFlightData)

    println("[Pipeline Step 3/9] Generating arrival data...")
    val enrichedWithArrival = FlightArrivalDataGenerator.preprocess(enrichedWithWBAN)

    println("[Pipeline Step 4/9] Generating flight features...")
    val generatedFlightData = FlightDataGenerator.preprocess(enrichedWithArrival)

    println("[Pipeline Step 5/9] Creating previous late flight features...")
    val generatedPreviousLateFlightData = FlightPreviousLateFlightFeatureGenerator.createLateAircraftFeature(generatedFlightData)

    // ⭐ CHECKPOINT 1: After heavy feature generation
    println("[Pipeline Step 5.5/9] Checkpointing after feature generation...")
    val checkpointed1 = generatedPreviousLateFlightData.checkpoint()
    checkpointed1.count()
    logMemoryUsage("After feature generation")

    println("[Pipeline Step 6/9] Generating labels...")
    val generatedFightDataWithLabels = FlightLabelGenerator.preprocess(checkpointed1)

    // ⭐ CHECKPOINT 2: After label generation
    println("[Pipeline Step 6.5/9] Checkpointing after label generation...")
    val checkpointed2 = generatedFightDataWithLabels.checkpoint()
    checkpointed2.count()
    logMemoryUsage("After label generation")

    // Calculate avg delay features for ALL delay thresholds (15min, 30min, 45min, 60min, 90min)
    println("[Pipeline Step 7/9] Calculating avg delay features...")
    val generatedFlightsWithAvgDelay = FlightAvgDelayFeatureGenerator.enrichFlightsWithAvgDelay(
      flightData = checkpointed2,
      sampleFraction = Some(0.3),  // Sample 30% pour éviter OOM
      enableCheckpoint = true
    )

    // ⭐ CHECKPOINT 3: After avg delay (memory intensive)
    println("[Pipeline Step 7.5/9] Checkpointing after avg delay...")
    val checkpointed3 = generatedFlightsWithAvgDelay.checkpoint()
    checkpointed3.count()
    logMemoryUsage("After avg delay")

    println("[Pipeline Step 8/9] Balancing data...")
    val finalCleanedData = FlightDataBalancer.preprocess(checkpointed3)

    // Validate schema
    println("[Pipeline Step 9/9] Validating schema...")
    validatePreprocessedSchema(finalCleanedData)

    // ⭐ OPTIMIZED WRITE STRATEGY
    writeFlightDataSafely(finalCleanedData, processedParquetPath)

    // Cleanup checkpoints
    checkpointed1.unpersist()
    checkpointed2.unpersist()
    checkpointed3.unpersist()

    println("\n" + "=" * 80)
    println("[Preprocessing] Flight Data Preprocessing Pipeline - End")
    println("=" * 80 + "\n")

    // Relire depuis le disque pour retourner un DataFrame propre
    spark.read.parquet(processedParquetPath)
  }

  /**
   * Stratégie d'écriture sécurisée pour éviter les OOM sur les données flight
   */
  private def writeFlightDataSafely(
                                     flightDf: DataFrame,
                                     outputPath: String
                                   )(implicit spark: SparkSession): Unit = {

    import spark.implicits._

    println(s"\nSaving preprocessed flight data to parquet:")
    println(s"  - Path: $outputPath")
    println("  - Using optimized write strategy")

    logMemoryUsage("Before write")

    val rowCount = flightDf.count()
    println(s"  - Rows to write: ${rowCount}")

    // ⭐ CHECKPOINT FINAL avant write
    println("\n  [Step 1/3] Final checkpoint before write...")
    val checkpointed = flightDf
      .repartition(100, col("UTC_FL_DATE"))  // Partition par date
      .checkpoint()

    checkpointed.count()
    println("  ✓ Checkpoint completed")

    logMemoryUsage("After checkpoint")

    // ⭐ WRITE avec partitionnement
    println("\n  [Step 2/3] Writing to parquet with date partitioning...")
    try {
      checkpointed
        .write
        .mode("overwrite")
        .partitionBy("UTC_FL_DATE")           // ⭐ Partition par date UTC
        .option("compression", "snappy")      // ⭐ Snappy plus léger
        .option("maxRecordsPerFile", 500000)
        .parquet(outputPath)

      println("  ✓ Write completed successfully")

    } catch {
      case e: Exception =>
        println(s"  ✗ Write failed: ${e.getMessage}")
        println("\n  Attempting fallback: batch write strategy...")

        writeFlightInBatches(checkpointed, outputPath)
    }

    // Cleanup
    println("\n  [Step 3/3] Cleaning up...")
    checkpointed.unpersist()
    System.gc()

    logMemoryUsage("After write")

    println(s"\n  ✓ Flight data saved successfully")
  }

  /**
   * Fallback: écriture par batch pour les flights
   */
  private def writeFlightInBatches(
                                    flightDf: DataFrame,
                                    outputPath: String,
                                    batchSize: Int = 1000000  // 1M rows per batch
                                  )(implicit spark: SparkSession): Unit = {

    import spark.implicits._

    val totalRows = flightDf.count()
    val numBatches = Math.ceil(totalRows.toDouble / batchSize).toInt

    println(s"  Writing in ${numBatches} batches of ${batchSize} rows...")

    val withBatchNum = flightDf
      .withColumn("temp_row_id", monotonically_increasing_id())
      .withColumn("temp_batch_num", (col("temp_row_id") / batchSize).cast("int"))
      .cache()

    withBatchNum.count()

    (0 until numBatches).foreach { batchNum =>
      print(s"  Batch ${batchNum + 1}/${numBatches}... ")

      val batchData = withBatchNum
        .filter(col("temp_batch_num") === batchNum)
        .drop("temp_row_id", "temp_batch_num")

      batchData
        .coalesce(5)
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(s"${outputPath}/batch_${batchNum}")

      println("✓")

      if (batchNum % 2 == 1) {
        System.gc()
        Thread.sleep(300)
      }
    }

    withBatchNum.unpersist()

    println(s"  ✓ All ${numBatches} batches written")
  }

  /**
   * Validate the schema of preprocessed data
   * Ensures all required columns exist with correct data types
   */
  private def validatePreprocessedSchema(df: DataFrame): Unit = {
    println("\n" + "=" * 80)
    println("Schema Validation")
    println("=" * 80)

    // Required base columns (from raw data)
    val requiredBaseColumns = Map(
      "UTC_FL_DATE" -> DateType,
      "OP_CARRIER_AIRLINE_ID" -> IntegerType,
      "OP_CARRIER_FL_NUM" -> IntegerType,
      "ORIGIN_AIRPORT_ID" -> IntegerType,
      "DEST_AIRPORT_ID" -> IntegerType,
      "CRS_DEP_TIME" -> IntegerType,
      "CRS_ELAPSED_TIME" -> DoubleType
    )

    // Required generated columns (from preprocessing)
    val requiredGeneratedColumns = Seq(
      "feature_departure_hour",
      "feature_flight_month",
      "feature_flight_year",
      "feature_flight_quarter",
      "feature_flight_day_of_week",
      "feature_is_weekend",
      "feature_route_id",
      "feature_distance_category",
      "ORIGIN_WBAN",
      "ORIGIN_TIMEZONE",
      "DEST_WBAN",
      "DEST_TIMEZONE"
    )

    // Required label columns
    val requiredLabelColumns = Seq(
      "label_arr_delay_filled",
      "label_weather_delay_filled",
      "label_nas_delay_filled",
      "label_is_delayed_15min",
      "label_is_delayed_30min",
      "label_is_delayed_60min",
      "label_is_delayed_90min"
    )

    val schema = df.schema
    val availableColumns = df.columns.toSet
    var validationPassed = true

    // Validate base columns with types
    println("\nValidating base columns:")
    requiredBaseColumns.foreach { case (colName, expectedType) =>
      if (!availableColumns.contains(colName)) {
        println(s"  ✗ Missing column: $colName")
        validationPassed = false
      } else {
        val actualType = schema(colName).dataType
        if (actualType != expectedType) {
          println(s"  ✗ Wrong type for $colName: expected $expectedType, got $actualType")
          validationPassed = false
        } else {
          println(s"  ✓ $colName ($expectedType)")
        }
      }
    }

    // Validate generated columns
    println("\nValidating generated columns:")
    requiredGeneratedColumns.foreach { colName =>
      if (!availableColumns.contains(colName)) {
        println(s"  ✗ Missing column: $colName")
        validationPassed = false
      } else {
        println(s"  ✓ $colName")
      }
    }

    // Validate label columns
    println("\nValidating label columns:")
    requiredLabelColumns.foreach { colName =>
      if (!availableColumns.contains(colName)) {
        println(s"  ✗ Missing column: $colName")
        validationPassed = false
      } else {
        println(s"  ✓ $colName")
      }
    }

    // Summary
    println("\n" + "=" * 80)
    if (validationPassed) {
      println(s"✓ Schema Validation PASSED - ${df.columns.length} columns validated")
    } else {
      println("✗ Schema Validation FAILED")
      throw new RuntimeException("Schema validation failed. Check logs for details.")
    }
    println("=" * 80)
  }

  /**
   * Monitoring mémoire
   */
  private def logMemoryUsage(label: String): Unit = {
    val runtime = Runtime.getRuntime
    val maxMemory = runtime.maxMemory() / (1024 * 1024 * 1024)
    val totalMemory = runtime.totalMemory() / (1024 * 1024 * 1024)
    val freeMemory = runtime.freeMemory() / (1024 * 1024 * 1024)
    val usedMemory = totalMemory - freeMemory

    val usagePercent = ((usedMemory / maxMemory) * 100).toInt

    println(f"\n  [$label] Memory: ${usedMemory}%.2f GB / ${maxMemory}%.2f GB (${usagePercent}%%)")

    if (usagePercent > 85) {
      println(s"  ⚠️  WARNING: Memory at ${usagePercent}% - forcing GC...")
      System.gc()
      Thread.sleep(1000)
    }
  }
}