package com.flightdelay.data.loaders

import com.flightdelay.data.model.Flight
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.time.LocalDateTime
import scala.util.{Try, Success, Failure}

/**
 * Data loader specifically designed for flight data
 * Handles loading and preprocessing of flight information including:
 * - Flight schedules and actual times
 * - Carrier and aircraft information
 * - Delay calculations
 * - Data quality validation
 */
object FlightDataLoader extends DataLoader[Flight] {

  // ===========================================================================================
  // CONFIGURATION AND CONSTANTS
  // ===========================================================================================

  private val DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"
  private val ALTERNATIVE_DATE_FORMATS = List("yyyy-MM-dd", "MM/dd/yyyy HH:mm", "yyyy-MM-dd'T'HH:mm:ss")

  // Expected column names (can be configured)
  private val COLUMN_MAPPING = Map(
    "origin" -> "ORIGIN",
    "dest" -> "DEST",
    "crs_dep_time" -> "CRS_DEP_TIME",
    "dep_time" -> "DEP_TIME",
    "crs_arr_time" -> "CRS_ARR_TIME",
    "arr_time" -> "ARR_TIME",
    "carrier" -> "UNIQUE_CARRIER",
    "flight_num" -> "FL_NUM",
    "tail_num" -> "TAIL_NUM",
    "cancelled" -> "CANCELLED",
    "diverted" -> "DIVERTED"
  )

  // ===========================================================================================
  // SCHEMA DEFINITION
  // ===========================================================================================

  override def expectedSchema: StructType = StructType(Array(
    StructField("ORIGIN", StringType, nullable = false),
    StructField("DEST", StringType, nullable = false),
    StructField("CRS_DEP_TIME", StringType, nullable = false),
    StructField("DEP_TIME", StringType, nullable = true),
    StructField("CRS_ARR_TIME", StringType, nullable = false),
    StructField("ARR_TIME", StringType, nullable = true),
    StructField("UNIQUE_CARRIER", StringType, nullable = false),
    StructField("FL_NUM", StringType, nullable = false),
    StructField("TAIL_NUM", StringType, nullable = true),
    StructField("CANCELLED", IntegerType, nullable = false),
    StructField("DIVERTED", IntegerType, nullable = false),
    StructField("FL_DATE", StringType, nullable = false),
    StructField("YEAR", IntegerType, nullable = false),
    StructField("MONTH", IntegerType, nullable = false),
    StructField("DAY_OF_MONTH", IntegerType, nullable = false)
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
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("timestampFormat", DEFAULT_DATE_FORMAT)
        .option("multiline", "true")
        .option("escape", "\"")
        .csv(path)
    }.recoverWith {
      case ex: Exception =>
        println(s"Failed to load raw flight data from $path: ${ex.getMessage}")
        Failure(ex)
    }
  }

  /**
   * Load flight data with full preprocessing and transformation
   * @param path Path to flight data files
   * @param spark Implicit SparkSession
   * @return Try[DataFrame] containing processed flight data
   */
  override def load(path: String)(implicit spark: SparkSession): Try[DataFrame] = {
    for {
      rawDf <- loadRaw(path)
      _ = if (!validateSchema(rawDf)) throw new IllegalStateException("Schema validation failed")
      cleanedDf = cleanData(rawDf)
      processedDf = transformToFlightData(cleanedDf)
    } yield processedDf
  }

  /**
   * Load flight data with specific filters applied
   * @param path Path to flight data
   * @param filters Filters to apply (e.g., "year" -> 2023, "carrier" -> "AA")
   * @param spark Implicit SparkSession
   * @return Try[DataFrame] containing filtered flight data
   */
  override def loadWithFilters(path: String, filters: Map[String, Any])
                              (implicit spark: SparkSession): Try[DataFrame] = {
    load(path).map { df =>
      filters.foldLeft(df) { case (accDf, (columnName, value)) =>
        accDf.filter(col(columnName) === value)
      }
    }
  }

  // ===========================================================================================
  // SPECIALIZED LOADING METHODS
  // ===========================================================================================

  /**
   * Load flight data for specific date range
   * @param path Path to flight data
   * @param startDate Start date (inclusive)
   * @param endDate End date (inclusive)
   * @param spark Implicit SparkSession
   * @return Try[DataFrame] containing flights in date range
   */
  def loadByDateRange(path: String, startDate: LocalDateTime, endDate: LocalDateTime)
                     (implicit spark: SparkSession): Try[DataFrame] = {
    load(path).map { df =>
      df.filter(
        col("scheduled_departure_time").between(
          lit(startDate.toString),
          lit(endDate.toString)
        )
      )
    }
  }

  /**
   * Load flight data for specific carriers
   * @param path Path to flight data
   * @param carriers List of carrier codes to include
   * @param spark Implicit SparkSession
   * @return Try[DataFrame] containing flights for specified carriers
   */
  def loadByCarriers(path: String, carriers: List[String])
                    (implicit spark: SparkSession): Try[DataFrame] = {
    load(path).map { df =>
      df.filter(col("carrier").isin(carriers: _*))
    }
  }

  /**
   * Load flight data for specific airports (origin or destination)
   * @param path Path to flight data
   * @param airports List of airport codes
   * @param airportType Either "origin", "destination", or "both"
   * @param spark Implicit SparkSession
   * @return Try[DataFrame] containing flights for specified airports
   */
  def loadByAirports(path: String, airports: List[String], airportType: String = "both")
                    (implicit spark: SparkSession): Try[DataFrame] = {
    load(path).map { df =>
      airportType.toLowerCase match {
        case "origin" => df.filter(col("origin_airport").isin(airports: _*))
        case "destination" => df.filter(col("destination_airport").isin(airports: _*))
        case "both" => df.filter(
          col("origin_airport").isin(airports: _*) ||
            col("destination_airport").isin(airports: _*)
        )
        case _ => throw new IllegalArgumentException(s"Invalid airportType: $airportType")
      }
    }
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
    val requiredColumns = Set("ORIGIN", "DEST", "CRS_DEP_TIME", "CRS_ARR_TIME", "UNIQUE_CARRIER")
    val availableColumns = df.columns.toSet

    val hasRequiredColumns = requiredColumns.subsetOf(availableColumns)

    if (!hasRequiredColumns) {
      val missingColumns = requiredColumns -- availableColumns
      println(s"Missing required columns: ${missingColumns.mkString(", ")}")
    }

    hasRequiredColumns
  }

  /**
   * Clean and standardize flight data
   * @param rawDf Raw DataFrame from CSV
   * @return Cleaned DataFrame
   */
  override def cleanData(rawDf: DataFrame): DataFrame = {
    rawDf
      // Remove rows with missing essential data
      .filter(
        col("ORIGIN").isNotNull &&
          col("DEST").isNotNull &&
          col("CRS_DEP_TIME").isNotNull &&
          col("CRS_ARR_TIME").isNotNull
      )
      // Standardize airport codes
      .withColumn("ORIGIN", upper(trim(col("ORIGIN"))))
      .withColumn("DEST", upper(trim(col("DEST"))))
      .withColumn("UNIQUE_CARRIER", upper(trim(col("UNIQUE_CARRIER"))))
      // Handle cancelled and diverted flags
      .withColumn("CANCELLED",
        when(col("CANCELLED").isNull, 0).otherwise(col("CANCELLED")))
      .withColumn("DIVERTED",
        when(col("DIVERTED").isNull, 0).otherwise(col("DIVERTED")))
      // Remove invalid flights (same origin and destination)
      .filter(col("ORIGIN") =!= col("DEST"))
  }

  // ===========================================================================================
  // DATA TRANSFORMATION
  // ===========================================================================================

  /**
   * Transform cleaned raw data into Flight domain objects structure
   * @param cleanedDf Cleaned DataFrame
   * @return DataFrame with Flight schema
   */
  def transformToFlightData(cleanedDf: DataFrame): DataFrame = {
    cleanedDf
      .withColumnRenamed("ORIGIN", "origin_airport")
      .withColumnRenamed("DEST", "destination_airport")
      .withColumnRenamed("UNIQUE_CARRIER", "carrier")
      .withColumnRenamed("FL_NUM", "flight_number")
      .withColumnRenamed("TAIL_NUM", "tail_number")
      // Create datetime columns
      .withColumn("scheduled_departure_time",
        parseTimeColumn(col("FL_DATE"), col("CRS_DEP_TIME")))
      .withColumn("actual_departure_time",
        parseTimeColumn(col("FL_DATE"), col("DEP_TIME")))
      .withColumn("scheduled_arrival_time",
        parseTimeColumn(col("FL_DATE"), col("CRS_ARR_TIME")))
      .withColumn("actual_arrival_time",
        parseTimeColumn(col("FL_DATE"), col("ARR_TIME")))
      // Calculate delays
      .withColumn("departure_delay", calculateDelayMinutes(
        col("scheduled_departure_time"), col("actual_departure_time")))
      .withColumn("arrival_delay", calculateDelayMinutes(
        col("scheduled_arrival_time"), col("actual_arrival_time")))
      // Add flags
      .withColumn("cancelled", col("CANCELLED") === 1)
      .withColumn("diverted", col("DIVERTED") === 1)
      .withColumn("is_delayed", col("arrival_delay") >= 15)
      // Select final columns
      .select(
        "origin_airport", "destination_airport", "carrier", "flight_number", "tail_number",
        "scheduled_departure_time", "actual_departure_time",
        "scheduled_arrival_time", "actual_arrival_time",
        "departure_delay", "arrival_delay", "cancelled", "diverted", "is_delayed"
      )
  }

  // ===========================================================================================
  // UTILITY METHODS
  // ===========================================================================================

  /**
   * Parse time columns combining date and time information
   * @param dateCol Date column
   * @param timeCol Time column (can be HHMM format)
   * @return Timestamp column
   */
  private def parseTimeColumn(dateCol: org.apache.spark.sql.Column,
                              timeCol: org.apache.spark.sql.Column) = {
    // Handle HHMM format time conversion
    val paddedTime = lpad(timeCol, 4, "0")
    val hourMinute = concat(
      substring(paddedTime, 1, 2), lit(":"),
      substring(paddedTime, 3, 2)
    )
    to_timestamp(concat(dateCol, lit(" "), hourMinute), "yyyy-MM-dd HH:mm")
  }

  /**
   * Calculate delay in minutes between scheduled and actual times
   * @param scheduledCol Scheduled time column
   * @param actualCol Actual time column
   * @return Delay in minutes (null if actual time is missing)
   */
  private def calculateDelayMinutes(scheduledCol: org.apache.spark.sql.Column,
                                    actualCol: org.apache.spark.sql.Column) = {
    when(actualCol.isNotNull,
      (unix_timestamp(actualCol) - unix_timestamp(scheduledCol)) / 60
    ).otherwise(null)
  }

  /**
   * Get comprehensive statistics about flight data
   * @param df Flight DataFrame
   * @return Map containing various statistics
   */
  override def getDataStatistics(df: DataFrame): Map[String, Any] = {
    val totalFlights = df.count()
    val cancelledFlights = df.filter(col("cancelled") === true).count()
    val divertedFlights = df.filter(col("diverted") === true).count()
    val delayedFlights = df.filter(col("is_delayed") === true).count()

    val avgDelay = df.agg(avg("arrival_delay")).collect()(0).getAs[Double](0)
    val maxDelay = df.agg(max("arrival_delay")).collect()(0).getAs[Double](0)

    Map(
      "total_flights" -> totalFlights,
      "cancelled_flights" -> cancelledFlights,
      "diverted_flights" -> divertedFlights,
      "delayed_flights" -> delayedFlights,
      "delay_rate" -> (delayedFlights.toDouble / totalFlights * 100),
      "average_delay_minutes" -> avgDelay,
      "max_delay_minutes" -> maxDelay,
      "unique_carriers" -> df.select("carrier").distinct().count(),
      "unique_airports" -> df.select("origin_airport").union(df.select("destination_airport")).distinct().count()
    )
  }
}
