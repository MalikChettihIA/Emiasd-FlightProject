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

    // Execute preprocessing pipeline (each step creates a new DataFrame)
    debug("[Pipeline Step 2/9] Cleaning flight data...")
    val cleanedFlightData = FlightDataCleaner.preprocess(enrichedWithWBAN)

    debug("[Pipeline Step 3/9] Enriching with Datasets ...")
    val enrichedWithDataset = FlightDataSetFilterGenerator.preprocess(cleanedFlightData)

    debug("[Pipeline Step 4/9] Generating arrival data...")
    val enrichedWithArrival = FlightArrivalDataGenerator.preprocess(enrichedWithDataset)

    debug("[Pipeline Step 5/9] Generating flight features...")
    val generatedFlightData = FlightDataGenerator.preprocess(enrichedWithArrival)


    debug("[Pipeline Step 6/9] Generating labels...")
    val generatedFightDataWithLabels = FlightLabelGenerator.preprocess(generatedFlightData)



    // Validate schema
    debug("[Pipeline Step 9/9] Validating schema...")
    validatePreprocessedSchema(generatedFightDataWithLabels)

    // Relire depuis le disque pour retourner un DataFrame propre
    generatedFightDataWithLabels
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
          debug(s"   $colName ($expectedType)")
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
        debug(s"   $colName")
      }
    }

    // Validate label columns
    debug("Validating label columns:")
    requiredLabelColumns.foreach { colName =>
      if (!availableColumns.contains(colName)) {
        debug(s"  ✗ Missing column: $colName")
        validationPassed = false
      } else {
        debug(s"   $colName")
      }
    }

    // Summary
    debug("=" * 80)
    if (validationPassed) {
      debug(s" Schema Validation PASSED - ${df.columns.length} columns validated")
    } else {
      debug(" Schema Validation FAILED")
      throw new RuntimeException("Schema validation failed. Check logs for details.")
    }
    debug("=" * 80)
  }


}