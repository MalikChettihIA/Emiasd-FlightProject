package com.flightdelay.data.loaders

import com.flightdelay.config.AppConfiguration
import com.flightdelay.data.models.Flight
import io.netty.handler.codec.http2.Http2FrameWriter.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}

/**
 * Data loader specifically designed for flight data
 * Handles loading and preprocessing of flight information including:
 * - Flight schedules and delays
 * - Carrier and flight information
 * - Delay calculations and categorization
 * - Data quality validation
 */
object FlightDataLoader extends DataLoader[Flight] {

  // ===========================================================================================
  // CONFIGURATION AND CONSTANTS
  // ===========================================================================================

  private val DEFAULT_DATE_FORMAT = "yyyy-MM-dd"

  // Expected column names mapping to actual CSV columns
  private val COLUMN_MAPPING = Map(
    "flDate" -> "FL_DATE",
    "opCarrierAirlineId" -> "OP_CARRIER_AIRLINE_ID",
    "opCarrierFlNum" -> "OP_CARRIER_FL_NUM",
    "originAirportId" -> "ORIGIN_AIRPORT_ID",
    "destAirportId" -> "DEST_AIRPORT_ID",
    "crsDepTime" -> "CRS_DEP_TIME",
    "arrDelayNew" -> "ARR_DELAY_NEW",
    "canceled" -> "CANCELLED",
    "diverted" -> "DIVERTED",
    "crsElapsedTime" -> "CRS_ELAPSED_TIME",
    "weatherDelay" -> "WEATHER_DELAY",
    "nasDelay" -> "NAS_DELAY"
  )

  // ===========================================================================================
  // SCHEMA DEFINITION
  // ===========================================================================================

  override def expectedSchema: StructType = StructType(Array(
    StructField("FL_DATE", StringType, nullable = false),
    StructField("OP_CARRIER_AIRLINE_ID", IntegerType, nullable = false),
    StructField("OP_CARRIER_FL_NUM", IntegerType, nullable = false),
    StructField("ORIGIN_AIRPORT_ID", IntegerType, nullable = false),
    StructField("DEST_AIRPORT_ID", IntegerType, nullable = false),
    StructField("CRS_DEP_TIME", IntegerType, nullable = false),
    StructField("ARR_DELAY_NEW", DoubleType, nullable = true),
    StructField("CANCELLED", IntegerType, nullable = false),
    StructField("DIVERTED", IntegerType, nullable = false),
    StructField("CRS_ELAPSED_TIME", DoubleType, nullable = true),
    StructField("WEATHER_DELAY", DoubleType, nullable = true),
    StructField("NAS_DELAY", DoubleType, nullable = true)
  ))

  // ===========================================================================================
  // CORE LOADING METHODS
  // ===========================================================================================

  /**
   * Load raw flight data from CSV files
   * @param path Path to flight data files (can be directory or specific file)
   * @param spark Implicit SparkSession
   * @return Try[DataFrame] containing raw flight data
   */
  override def loadRaw(path: String)(implicit spark: SparkSession): Try[DataFrame] = {
    Try {
      spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("timestampFormat", DEFAULT_DATE_FORMAT)
        .option("multiline", "true")
        .option("escape", "\"")
        .load(path)
        .drop("_c12")
        .persist()
    }.recoverWith {
      case ex: Exception =>
        println(s"Failed to load raw flight data from $path: ${ex.getMessage}")
        Failure(ex)
    }
  }

  /**
   * Load flight data with full preprocessing and transformation
   * @param configuration Configuration de l'application
   * @param spark Implicit SparkSession
   * @return Try[DataFrame] containing processed flight data
   */
  override def load(configuration: AppConfiguration, validate: Boolean = false)(implicit spark: SparkSession): Try[DataFrame] = {
    println("--> Flight Data Loading ...")
    for {
      rawDf <- loadRaw(configuration.data.flight.path)
      //if validate
      //  _ = if (!validateSchema(rawDf)) throw new IllegalStateException("Schema validation failed")
    } yield rawDf
  }


  // ===========================================================================================
  // DATA VALIDATION AND CLEANING
  // ===========================================================================================

  /**
   * Validate that the DataFrame has the expected schema structure
   * @param df DataFrame to validate
   * @return Boolean indicating if schema is valid
   */
  override def validateSchema(df: DataFrame): Boolean = {
    val requiredColumns = Set(
        "FL_DATE",
        "OP_CARRIER_AIRLINE_ID",
        "OP_CARRIER_FL_NUM",
        "ORIGIN_AIRPORT_ID",
        "DEST_AIRPORT_ID",
        "ARR_DELAY_NEW"
    )
    val availableColumns = df.columns.toSet

    val hasRequiredColumns = requiredColumns.subsetOf(availableColumns)

    if (!hasRequiredColumns) {
      val missingColumns = requiredColumns -- availableColumns
      println(s"Missing required columns: ${missingColumns.mkString(", ")}")
    }

    hasRequiredColumns
  }

}