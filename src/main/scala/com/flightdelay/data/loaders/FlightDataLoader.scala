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

  private def expectedSchema: StructType = StructType(Array(
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
   * Load flight data with full preprocessing and transformation
   * @param configuration Configuration de l'application
   * @param spark Implicit SparkSession
   * @return Try[DataFrame] containing processed flight data
   */
  override def loadFromConfiguration(validate: Boolean = false)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {
    val filePath = configuration.data.flight.path;
    loadFromFilePath(filePath, validate)
  }


  /**
   * Load flight data with full preprocessing and transformation
   * @param configuration Configuration de l'application
   * @param spark Implicit SparkSession
   * @return Try[DataFrame] containing processed flight data
   */
  override def loadFromFilePath(filePath: String, validate: Boolean = false)(implicit spark: SparkSession): DataFrame = {
    println("")
    println("")
    println("----------------------------------------------------------------------------------------------------------")
    println("--> [FlightDataLoader] Flight Data Loading - Start ...")

    val rawDf = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("timestampFormat", DEFAULT_DATE_FORMAT)
      .option("multiline", "true")
      .option("escape", "\"")
      .load(filePath)
      .drop("_c12")
      .persist()

    println("--> "+rawDf.count+" loaded ...")
    rawDf.printSchema
    rawDf.show(10)

    if (validate && (!validateSchema(rawDf)))
      println("Schema validation failed")
    println("--> [FlightDataLoader] Flight Data Loading - End ...")
    println("----------------------------------------------------------------------------------------------------------")
    println("")
    println("")
    rawDf
  }

  // ===========================================================================================
  // DATA VALIDATION AND CLEANING
  // ===========================================================================================

  /**
   * Validate that the DataFrame has the expected schema structure
   * @param df DataFrame to validate
   * @return Boolean indicating if schema is valid
   */
  private def validateSchema(df: DataFrame): Boolean = {
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