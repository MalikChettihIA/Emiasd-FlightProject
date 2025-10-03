package com.flightdelay.data.preprocessing

import com.flightdelay.config.AppConfiguration
import com.flightdelay.utils.CsvWriter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

object FlightPreprocessingPipeline {

  /**
   * Exécute le pipeline complet de preprocessing
   * Charge les données depuis le fichier parquet généré par FlightDataLoader
   * @param configuration Configuration contenant les paramètres de chargement
   * @param spark Session Spark
   * @return DataFrame complètement préprocessé avec labels
   */
  def execute()(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    println("")
    println("")
    println("----------------------------------------------------------------------------------------------------------")
    println("--> [FlightPreprocessingPipeline] Flight Preprocessing Pipeline - Start ...")
    println("----------------------------------------------------------------------------------------------------------")

    val processedParquetPath = s"${configuration.output.data.path}/processed_flights.parquet"

    // Load raw data from parquet
    val rawParquetPath = s"${configuration.output.data.path}/raw_flights.parquet"
    println(s"--> Loading raw data from parquet: $rawParquetPath")
    val originalDf = spark.read.parquet(rawParquetPath)
    println(s"--> ✓ Loaded ${originalDf.count()} raw records from parquet")

    // Execute preprocessing pipeline
    val cleanedFlightData = FlightDataCleaner.preprocess(originalDf)
    val generatedFightData = FlightDataGenerator.preprocess(cleanedFlightData)
    val generatedFightDataWithLabels = FlightLabelGenerator.preprocess(generatedFightData)
    val finalCleanedData = FlightDataLeakageCleaner.preprocess(generatedFightDataWithLabels)

    // Validate schema
    validatePreprocessedSchema(finalCleanedData)

    // Save processed data to parquet
    println(s"--> Saving preprocessed data to parquet: $processedParquetPath")
    finalCleanedData.write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(processedParquetPath)
    println(s"--> ✓ Saved ${finalCleanedData.count()} preprocessed records to parquet")

    println("")
    println("----------------------------------------------------------------------------------------------------------")
    println("--> [FlightPreprocessingPipeline] Flight Preprocessing Pipeline - End ...")
    println("----------------------------------------------------------------------------------------------------------")

    finalCleanedData
  }

  /**
   * Validate the schema of preprocessed data
   * Ensures all required columns exist with correct data types
   */
  private def validatePreprocessedSchema(df: DataFrame): Unit = {
    println("")
    println("----------------------------------------------------------------------------------------------------------")
    println("--> Schema Validation")
    println("----------------------------------------------------------------------------------------------------------")

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
      "feature_distance_category"
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
    println("\n✓ Validating base columns:")
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
    println("\n✓ Validating generated columns:")
    requiredGeneratedColumns.foreach { colName =>
      if (!availableColumns.contains(colName)) {
        println(s"  ✗ Missing column: $colName")
        validationPassed = false
      } else {
        println(s"  ✓ $colName")
      }
    }

    // Validate label columns
    println("\n✓ Validating label columns:")
    requiredLabelColumns.foreach { colName =>
      if (!availableColumns.contains(colName)) {
        println(s"  ✗ Missing column: $colName")
        validationPassed = false
      } else {
        println(s"  ✓ $colName")
      }
    }

    // Validate that leakage columns have been removed
    println("\n✓ Validating leakage columns removal:")
    val leakageColumns = Seq("ARR_DELAY_NEW", "WEATHER_DELAY", "NAS_DELAY")
    leakageColumns.foreach { colName =>
      if (availableColumns.contains(colName)) {
        println(s"  ✗ LEAKAGE DETECTED: $colName should have been removed!")
        validationPassed = false
      } else {
        println(s"  ✓ $colName removed (no leakage)")
      }
    }

    // Summary
    println("\n----------------------------------------------------------------------------------------------------------")
    if (validationPassed) {
      println(s"✓ Schema validation PASSED - ${df.columns.length} columns validated")
    } else {
      println("✗ Schema validation FAILED")
      throw new RuntimeException("Schema validation failed. Check logs for details.")
    }
    println("----------------------------------------------------------------------------------------------------------")
    println("")

    // Print schema for reference
    println("Full schema:")
    df.printSchema()
  }

}