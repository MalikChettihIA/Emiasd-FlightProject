package com.flightdelay.data.preprocessing.flights

import com.flightdelay.config.{AppConfiguration, ExperimentConfig}
import com.flightdelay.data.preprocessing.weather.WeatherInteractionFeatures
import com.flightdelay.data.utils.TimeFeatureUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

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

    // Execute preprocessing pipeline
    val cleanedFlightData = FlightDataCleaner.preprocess(originalDf)
    val enrichedWithWBAN = FlightWBANEnricher.preprocess(cleanedFlightData)
    val generatedFightData = FlightDataGenerator.preprocess(enrichedWithWBAN)
    val generatedFightDataWithLabels = FlightLabelGenerator.preprocess(generatedFightData)
    val flightLeakageCleanedData = FlightDataLeakageCleaner.preprocess(generatedFightDataWithLabels)
    val finalCleanedData = FlightDataBalancer.preprocess(flightLeakageCleanedData)

    // Validate schema
    validatePreprocessedSchema(finalCleanedData)

    // Save processed data to parquet
    println(s"\nSaving preprocessed data to parquet:")
    println(s"  - Path: $processedParquetPath")
    finalCleanedData.write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(processedParquetPath)
    println(s"  - Saved ${finalCleanedData.count()} preprocessed records")

    println("\n" + "=" * 80)
    println("[Preprocessing] Flight Data Preprocessing Pipeline - End")
    println("=" * 80 + "\n")

    finalCleanedData
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
    // Note: ARR_DELAY_NEW, WEATHER_DELAY, NAS_DELAY are removed by FlightDataLeakageCleaner
    val requiredBaseColumns = Map(
      "FL_DATE" -> DateType,
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
      "label_is_delayed_60min"
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
          println(s"  - $colName ($expectedType)")
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
        println(s"  - $colName")
      }
    }

    // Validate label columns
    println("\nValidating label columns:")
    requiredLabelColumns.foreach { colName =>
      if (!availableColumns.contains(colName)) {
        println(s"  ✗ Missing column: $colName")
        validationPassed = false
      } else {
        println(s"  - $colName")
      }
    }

    // Validate that leakage columns have been removed
    println("\nValidating leakage columns removal:")
    val leakageColumns = Seq("ARR_DELAY_NEW", "WEATHER_DELAY", "NAS_DELAY")
    leakageColumns.foreach { colName =>
      if (availableColumns.contains(colName)) {
        println(s"  ✗ LEAKAGE DETECTED: $colName should have been removed!")
        validationPassed = false
      } else {
        println(s"  - $colName removed (no leakage)")
      }
    }

    // Summary
    println("\n" + "=" * 80)
    if (validationPassed) {
      println(s"- Schema Validation PASSED - ${df.columns.length} columns validated")
    } else {
      println("✗ Schema Validation FAILED")
      throw new RuntimeException("Schema validation failed. Check logs for details.")
    }
    println("=" * 80)
  }

}