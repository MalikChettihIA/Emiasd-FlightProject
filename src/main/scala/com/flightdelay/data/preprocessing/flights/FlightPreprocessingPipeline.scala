package com.flightdelay.data.preprocessing.flights

import com.flightdelay.config.AppConfiguration
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.flightdelay.utils.DebugUtils._

object FlightPreprocessingPipeline {

  /**
   * Exécute le pipeline complet de preprocessing
   * Charge les données depuis le fichier parquet généré par FlightDataLoader
   * @param configuration Configuration contenant les paramètres de chargement
   * @param spark Session Spark
   * @return DataFrame complètement préprocessé avec labels
   */
  def execute()(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    info("=" * 80)
    info("[DataPipeline][Step 4/7] Flight Data Preprocessing Pipeline - Start")
    info("=" * 80)

    val processedParquetPath = s"${configuration.common.output.basePath}/common/data/processed_flights.parquet"

    // Load raw data from parquet
    val rawParquetPath = s"${configuration.common.output.basePath}/common/data/raw_flights.parquet"
    debug(s"Loading raw data from parquet:")
    debug(s"  - Path: $rawParquetPath")
    val originalDf = spark.read.parquet(rawParquetPath)
    debug(s"  - Loaded ${originalDf.count()} raw records")

    debug("[Pipeline Step 1/9] Enriching with WBAN...")
    val enrichedWithWBAN = FlightWBANEnricher.preprocess(originalDf)

    val enrichedWithWBANPath = s"${configuration.common.output.basePath}/common/data/wban_enriched_flights.parquet"
    debug(f"[Pipeline Step 1/9] Wrinting Enriched parquer file... {enrichedWithWBANPath}")
    enrichedWithWBAN
      .write.mode("overwrite")
      .parquet(enrichedWithWBANPath)

    // Execute preprocessing pipeline (each step creates a new DataFrame)
    debug("[Pipeline Step 2/9] Cleaning flight data...")
    val cleanedFlightData = FlightDataCleaner.preprocess(enrichedWithWBAN)

    debug("[Pipeline Step 3/9] Enriching with Datasets ...")
    val enrichedWithDataset = FlightDataSetFilterGenerator.preprocess(cleanedFlightData)

    debug("[Pipeline Step 4/9] Generating arrival data...")
    val enrichedWithArrival = FlightArrivalDataGenerator.preprocess(enrichedWithDataset)

    debug("[Pipeline Step 5/9] Generating flight features...")
    val generatedFlightData = FlightDataGenerator.preprocess(enrichedWithArrival)

    //debug("[Pipeline Step 5/9] Creating previous late flight features...")
    //val generatedPreviousLateFlightData = FlightPreviousLateFlightFeatureGenerator.createLateAircraftFeature(generatedFlightData)

    // ⭐ CHECKPOINT 1: After heavy feature generation
    //debug("[Pipeline Step 5.5/9] Checkpointing after feature generation...")
    //val checkpointed1 = generatedPreviousLateFlightData.checkpoint()
    //checkpointed1.count()
    //logMemoryUsage("After feature generation")

    debug("[Pipeline Step 6/9] Generating labels...")
    //val generatedFightDataWithLabels = FlightLabelGenerator.preprocess(checkpointed1)
    val generatedFightDataWithLabels = FlightLabelGenerator.preprocess(generatedFlightData)

    // ⭐ CHECKPOINT 2: After label generation
    //debug("[Pipeline Step 6.5/9] Checkpointing after label generation...")
    //val checkpointed2 = generatedFightDataWithLabels.checkpoint()
    //checkpointed2.count()
    //logMemoryUsage("After label generation")

    // Calculate avg delay features for ALL delay thresholds (15min, 30min, 45min, 60min, 90min)
    //debug("[Pipeline Step 7/9] Calculating avg delay features...")
    //val generatedFlightsWithAvgDelay = FlightAvgDelayFeatureGenerator.enrichFlightsWithAvgDelay(
    //  flightData = checkpointed2,
    //  enableCheckpoint = true
    //)

    // ⭐ CHECKPOINT 3: After avg delay (memory intensive)
    //debug("[Pipeline Step 7.5/9] Checkpointing after avg delay...")
    //val checkpointed3 = generatedFlightsWithAvgDelay.checkpoint()
    //checkpointed3.count()
    //logMemoryUsage("After avg delay")

    // Validate schema
    debug("[Pipeline Step 9/9] Validating schema...")
    validatePreprocessedSchema(generatedFightDataWithLabels)

    // ⭐ OPTIMIZED WRITE STRATEGY
    writeFlightDataSafely(generatedFightDataWithLabels, processedParquetPath)

    // Cleanup checkpoints
    //checkpointed1.unpersist()
    //checkpointed2.unpersist()
    //checkpointed3.unpersist()

    // Relire depuis le disque pour retourner un DataFrame propre
    spark.read.parquet(processedParquetPath)
  }

  /**
   * Stratégie d'écriture sécurisée pour éviter les OOM sur les données flight
   */
  private def writeFlightDataSafely(
                                     flightDf: DataFrame,
                                     outputPath: String
                                   )(implicit sparkSession: SparkSession, configuration: AppConfiguration): Unit = {

    debug(s"Saving preprocessed flight data to parquet:")
    debug(s"  - Path: $outputPath")
    debug("  - Using optimized write strategy")

    logMemoryUsage("Before write")

    val rowCount = flightDf.count()
    debug(s"  - Rows to write: ${rowCount}")

    // ⭐ CHECKPOINT FINAL avant write
    debug("[Step 1/3] Final checkpoint before write...")
    val checkpointed = flightDf
      .repartition(100, col("UTC_FL_DATE"))  // Partition par date
      .checkpoint()

    checkpointed.count()
    debug("  ✓ Checkpoint completed")

    logMemoryUsage("After checkpoint")

    // ⭐ WRITE avec partitionnement
    debug("[Step 2/3] Writing to parquet with date partitioning...")
    try {
      checkpointed
        .write
        .mode("overwrite")
        .partitionBy("UTC_FL_DATE")           // ⭐ Partition par date UTC
        .option("compression", "snappy")      // ⭐ Snappy plus léger
        .option("maxRecordsPerFile", 500000)
        .parquet(outputPath)

      debug("  ✓ Write completed successfully")

    } catch {
      case e: Exception =>
        debug(s"  ✗ Write failed: ${e.getMessage}")
        debug("  Attempting fallback: batch write strategy...")

        writeFlightInBatches(checkpointed, outputPath)
    }

    // Cleanup
    debug("  [Step 3/3] Cleaning up...")
    checkpointed.unpersist()
    System.gc()

    logMemoryUsage("After write")

    debug(s"   Flight data saved successfully")
  }

  /**
   * Fallback: écriture par batch pour les flights
   */
  private def writeFlightInBatches(
                                    flightDf: DataFrame,
                                    outputPath: String,
                                    batchSize: Int = 1000000  // 1M rows per batch
                                  )(implicit sparkSession: SparkSession, configuration: AppConfiguration): Unit = {

    val totalRows = flightDf.count()
    val numBatches = Math.ceil(totalRows.toDouble / batchSize).toInt

    debug(s"  Writing in ${numBatches} batches of ${batchSize} rows...")

    val withBatchNum = flightDf
      .withColumn("temp_row_id", monotonically_increasing_id())
      .withColumn("temp_batch_num", (col("temp_row_id") / batchSize).cast("int"))
      .cache()

    withBatchNum.count()

    (0 until numBatches).foreach { batchNum =>
      debug(s"  Batch ${batchNum + 1}/${numBatches}... ")

      val batchData = withBatchNum
        .filter(col("temp_batch_num") === batchNum)
        .drop("temp_row_id", "temp_batch_num")

      batchData
        .coalesce(5)
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(s"${outputPath}/batch_${batchNum}")

      debug("✓")

      if (batchNum % 2 == 1) {
        System.gc()
        Thread.sleep(300)
      }
    }

    withBatchNum.unpersist()

    debug(s"  ✓ All ${numBatches} batches written")
  }

  /**
   * Validate the schema of preprocessed data
   * Ensures all required columns exist with correct data types
   */
  private def validatePreprocessedSchema(df: DataFrame)(implicit sparkSession: SparkSession, configuration: AppConfiguration): Unit = {
    debug("" + "=" * 80)
    debug("Schema Validation")
    debug("=" * 80)

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
    debug("Validating base columns:")
    requiredBaseColumns.foreach { case (colName, expectedType) =>
      if (!availableColumns.contains(colName)) {
        debug(s"  ✗ Missing column: $colName")
        validationPassed = false
      } else {
        val actualType = schema(colName).dataType
        if (actualType != expectedType) {
          debug(s"  ✗ Wrong type for $colName: expected $expectedType, got $actualType")
          validationPassed = false
        } else {
          debug(s"  ✓ $colName ($expectedType)")
        }
      }
    }

    // Validate generated columns
    debug("Validating generated columns:")
    requiredGeneratedColumns.foreach { colName =>
      if (!availableColumns.contains(colName)) {
        debug(s"  ✗ Missing column: $colName")
        validationPassed = false
      } else {
        debug(s"  ✓ $colName")
      }
    }

    // Validate label columns
    debug("Validating label columns:")
    requiredLabelColumns.foreach { colName =>
      if (!availableColumns.contains(colName)) {
        debug(s"  ✗ Missing column: $colName")
        validationPassed = false
      } else {
        debug(s"  ✓ $colName")
      }
    }

    // Summary
    debug("=" * 80)
    if (validationPassed) {
      debug(s"✓ Schema Validation PASSED - ${df.columns.length} columns validated")
    } else {
      debug("✗ Schema Validation FAILED")
      throw new RuntimeException("Schema validation failed. Check logs for details.")
    }
    debug("=" * 80)
  }

  /**
   * Monitoring mémoire
   */
  private def logMemoryUsage(label: String)(implicit sparkSession: SparkSession, configuration: AppConfiguration): Unit = {
    val runtime = Runtime.getRuntime
    val maxMemory = runtime.maxMemory() / (1024 * 1024 * 1024)
    val totalMemory = runtime.totalMemory() / (1024 * 1024 * 1024)
    val freeMemory = runtime.freeMemory() / (1024 * 1024 * 1024)
    val usedMemory = totalMemory - freeMemory

    val usagePercent = ((usedMemory / maxMemory) * 100).toInt

    debug(f"  [$label] Memory: ${usedMemory}%.2f GB / ${maxMemory}%.2f GB (${usagePercent}%%)")

    if (usagePercent > 85) {
      debug(s"  ⚠️  WARNING: Memory at ${usagePercent}% - forcing GC...")
      System.gc()
      Thread.sleep(1000)
    }
  }
}