package com.flightdelay.data.loaders

import com.flightdelay.data.model.WeatherObservation
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.time.LocalDateTime
import scala.util.{Try, Success, Failure}

/**
 * Data loader for weather observation data
 * Handles loading and preprocessing of weather information including:
 * - Temperature, humidity, pressure readings
 * - Wind conditions and visibility
 * - Weather phenomena and sky conditions
 * - Quality control and data validation
 */
object WeatherDataLoader extends DataLoader[WeatherObservation] {

  // ===========================================================================================
  // CONFIGURATION AND CONSTANTS
  // ===========================================================================================

  // Weather data often comes in fixed-width format or specialized CSV
  private val WEATHER_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"
  private val ISD_DATE_FORMAT = "yyyyMMddHHmm"  // Integrated Surface Data format

  // Quality control flags
  private val VALID_QUALITY_FLAGS = Set("0", "1", "4", "5", "9")
  private val MISSING_VALUE_INDICATORS = Set("999.9", "9999", "-9999", "")

  // ===========================================================================================
  // SCHEMA DEFINITION
  // ===========================================================================================

  override def expectedSchema: StructType = StructType(Array(
    StructField("STATION", StringType, nullable = false),
    StructField("DATE", StringType, nullable = false),
    StructField("LATITUDE", DoubleType, nullable = true),
    StructField("LONGITUDE", DoubleType, nullable = true),
    StructField("ELEVATION", DoubleType, nullable = true),
    StructField("NAME", StringType, nullable = true),
    StructField("TEMP", DoubleType, nullable = true),
    StructField("TEMP_QUALITY", StringType, nullable = true),
    StructField("DEWP", DoubleType, nullable = true),
    StructField("DEWP_QUALITY", StringType, nullable = true),
    StructField("SLP", DoubleType, nullable = true),
    StructField("SLP_QUALITY", StringType, nullable = true),
    StructField("VISIB", DoubleType, nullable = true),
    StructField("VISIB_QUALITY", StringType, nullable = true),
    StructField("WDSP", DoubleType, nullable = true),
    StructField("WDSP_QUALITY", StringType, nullable = true),
    StructField("MXSPD", DoubleType, nullable = true),
    StructField("GUST", DoubleType, nullable = true),
    StructField("MAX", DoubleType, nullable = true),
    StructField("MIN", DoubleType, nullable = true),
    StructField("PRCP", DoubleType, nullable = true),
    StructField("SNDP", DoubleType, nullable = true),
    StructField("FRSHTT", StringType, nullable = true)
  ))

  // ===========================================================================================
  // CORE LOADING METHODS
  // ===========================================================================================

  /**
   * Load raw weather data from various formats (CSV, fixed-width, etc.)
   * @param path Path to weather data files
   * @param spark Implicit SparkSession
   * @return Try[DataFrame] containing raw weather data
   */
  override def loadRaw(path: String)(implicit spark: SparkSession): Try[DataFrame] = {
    Try {
      // Detect file format and load accordingly
      if (path.contains("isd") || path.contains("ISD")) {
        loadISDFormat(path)
      } else {
        // Standard CSV format
        spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .option("timestampFormat", WEATHER_DATE_FORMAT)
          .option("multiline", "true")
          .csv(path)
      }
    }.recoverWith {
      case ex: Exception =>
        println(s"Failed to load raw weather data from $path: ${ex.getMessage}")
        Failure(ex)
    }
  }

  /**
   * Load weather data with full preprocessing
   * @param path Path to weather data
   * @param spark Implicit SparkSession
   * @return Try[DataFrame] containing processed weather data
   */
  override def load(path: String)(implicit spark: SparkSession): Try[DataFrame] = {
    for {
      rawDf <- loadRaw(path)
      _ = if (!validateSchema(rawDf)) println("Warning: Schema validation failed, proceeding with available columns")
      cleanedDf = cleanData(rawDf)
      processedDf = transformToWeatherData(cleanedDf)
    } yield processedDf
  }

  /**
   * Load weather data with filters (e.g., by station, date range, quality)
   * @param path Path to weather data
   * @param filters Filters to apply
   * @param spark Implicit SparkSession
   * @return Try[DataFrame] containing filtered weather data
   */
  override def loadWithFilters(path: String, filters: Map[String, Any])
                              (implicit spark: SparkSession): Try[DataFrame] = {
    load(path).map { df =>
      filters.foldLeft(df) { case (accDf, (columnName, value)) =>
        value match {
          case list: List[_] => accDf.filter(col(columnName).isin(list: _*))
          case single => accDf.filter(col(columnName) === single)
        }
      }
    }
  }

  // ===========================================================================================
  // SPECIALIZED LOADING METHODS
  // ===========================================================================================

  /**
   * Load weather data for specific airports/stations
   * @param path Path to weather data
   * @param stationIds List of weather station IDs
   * @param spark Implicit SparkSession
   * @return Try[DataFrame] containing weather data for specified stations
   */
  def loadByStations(path: String, stationIds: List[String])
                    (implicit spark: SparkSession): Try[DataFrame] = {
    loadWithFilters(path, Map("station" -> stationIds))
  }

  /**
   * Load weather data for specific date range
   * @param path Path to weather data
   * @param startDate Start date
   * @param endDate End date
   * @param spark Implicit SparkSession
   * @return Try[DataFrame] containing weather data in date range
   */
  def loadByDateRange(path: String, startDate: LocalDateTime, endDate: LocalDateTime)
                     (implicit spark: SparkSession): Try[DataFrame] = {
    load(path).map { df =>
      df.filter(
        col("observation_time").between(
          lit(startDate.toString),
          lit(endDate.toString)
        )
      )
    }
  }

  /**
   * Load weather data with quality control filters
   * @param path Path to weather data
   * @param minQuality Minimum quality level to include
   * @param spark Implicit SparkSession
   * @return Try[DataFrame] containing high-quality weather data
   */
  def loadHighQualityData(path: String, minQuality: String = "1")
                         (implicit spark: SparkSession): Try[DataFrame] = {
    load(path).map { df =>
      df.filter(
        col("temp_quality").isin(VALID_QUALITY_FLAGS.toSeq: _*) &&
          col("pressure_quality").isin(VALID_QUALITY_FLAGS.toSeq: _*) &&
          col("visibility_quality").isin(VALID_QUALITY_FLAGS.toSeq: _*)
      )
    }
  }

  // ===========================================================================================
  // SPECIALIZED FORMAT LOADERS
  // ===========================================================================================

  /**
   * Load Integrated Surface Data (ISD) format files
   * @param path Path to ISD files
   * @param spark Implicit SparkSession
   * @return DataFrame with parsed ISD data
   */
  private def loadISDFormat(path: String)(implicit spark: SparkSession): DataFrame = {
    // ISD files are fixed-width format - need special parsing
    val rawText = spark.read.text(path)

    rawText.select(
      // Parse fixed positions according to ISD format
      substring(col("value"), 5, 6).alias("STATION"),
      substring(col("value"), 16, 12).alias("DATE"),
      substring(col("value"), 29, 6).cast(DoubleType).divide(1000).alias("LATITUDE"),
      substring(col("value"), 35, 7).cast(DoubleType).divide(1000).alias("LONGITUDE"),
      substring(col("value"), 47, 5).cast(DoubleType).alias("ELEVATION"),
      substring(col("value"), 88, 5).cast(DoubleType).divide(10).alias("TEMP"),
      substring(col("value"), 93, 1).alias("TEMP_QUALITY"),
      substring(col("value"), 94, 5).cast(DoubleType).divide(10).alias("DEWP"),
      substring(col("value"), 99, 1).alias("DEWP_QUALITY"),
      substring(col("value"), 100, 5).cast(DoubleType).divide(10).alias("SLP"),
      substring(col("value"), 105, 1).alias("SLP_QUALITY")
      // Add more fields as needed based on ISD format specification
    ).filter(length(col("value")) > 100) // Filter out incomplete records
  }

  // ===========================================================================================
  // DATA VALIDATION AND CLEANING
  // ===========================================================================================

  /**
   * Validate weather data schema (more flexible than flight data)
   * @param df DataFrame to validate
   * @return Boolean indicating if schema is adequate
   */
  override def validateSchema(df: DataFrame): Boolean = {
    val requiredColumns = Set("STATION", "DATE")
    val availableColumns = df.columns.toSet

    val hasMinimumColumns = requiredColumns.subsetOf(availableColumns)

    // Check for at least some weather measurement columns
    val weatherColumns = Set("TEMP", "SLP", "VISIB", "WDSP").intersect(availableColumns)
    val hasWeatherData = weatherColumns.nonEmpty

    if (!hasMinimumColumns) {
      println(s"Missing required columns: ${requiredColumns -- availableColumns}")
    }
    if (!hasWeatherData) {
      println("No weather measurement columns found")
    }

    hasMinimumColumns && hasWeatherData
  }

  /**
   * Clean and standardize weather data
   * @param rawDf Raw weather DataFrame
   * @return Cleaned DataFrame
   */
  override def cleanData(rawDf: DataFrame): DataFrame = {
    rawDf
      // Remove rows with missing essential data
      .filter(col("STATION").isNotNull && col("DATE").isNotNull)
      // Standardize station IDs
      .withColumn("STATION", upper(trim(col("STATION"))))
      // Handle missing values and quality flags
      .withColumn("TEMP",
        when(col("TEMP").isin(MISSING_VALUE_INDICATORS.toSeq: _*), null)
          .otherwise(col("TEMP")))
      .withColumn("SLP",
        when(col("SLP").isin(MISSING_VALUE_INDICATORS.toSeq: _*), null)
          .otherwise(col("SLP")))
      .withColumn("VISIB",
        when(col("VISIB").isin(MISSING_VALUE_INDICATORS.toSeq: _*), null)
          .otherwise(col("VISIB")))
      .withColumn("WDSP",
        when(col("WDSP").isin(MISSING_VALUE_INDICATORS.toSeq: _*), null)
          .otherwise(col("WDSP")))
      // Filter out rows with low quality data
      .filter(
        col("TEMP_QUALITY").isNull ||
          col("TEMP_QUALITY").isin(VALID_QUALITY_FLAGS.toSeq: _*)
      )
  }

  // ===========================================================================================
  // DATA TRANSFORMATION
  // ===========================================================================================

  /**
   * Transform cleaned weather data into WeatherObservation schema
   * @param cleanedDf Cleaned DataFrame
   * @return DataFrame with WeatherObservation schema
   */
  def transformToWeatherData(cleanedDf: DataFrame): DataFrame = {
    cleanedDf
      .withColumnRenamed("STATION", "station")
      // Parse date/time
      .withColumn("observation_time",
        when(length(col("DATE")) === 12,
          to_timestamp(col("DATE"), ISD_DATE_FORMAT))
          .otherwise(to_timestamp(col("DATE"), WEATHER_DATE_FORMAT)))
      // Rename and convert weather measurements
      .withColumn("temperature", col("TEMP"))
      .withColumn("humidity", calculateHumidity(col("TEMP"), col("DEWP")))
      .withColumn("pressure", col("SLP"))
      .withColumn("visibility", col("VISIB"))
      .withColumn("wind_speed", col("WDSP"))
      .withColumn("wind_direction", lit(null).cast(DoubleType)) // Often not available in basic datasets
      // Quality flags
      .withColumn("temp_quality", col("TEMP_QUALITY"))
      .withColumn("pressure_quality", col("SLP_QUALITY"))
      .withColumn("visibility_quality", col("VISIB_QUALITY"))
      // Weather phenomena parsing (if available)
      .withColumn("weather_phenomena", parseWeatherPhenomena(col("FRSHTT")))
      // Select final columns
      .select(
        "station", "observation_time", "temperature", "humidity",
        "pressure", "visibility", "wind_speed", "wind_direction",
        "weather_phenomena", "temp_quality", "pressure_quality", "visibility_quality"
      )
  }

  // ===========================================================================================
  // UTILITY METHODS
  // ===========================================================================================

  /**
   * Calculate relative humidity from temperature and dew point
   * @param temp Temperature in Celsius
   * @param dewp Dew point in Celsius
   * @return Relative humidity percentage
   */
  private def calculateHumidity(temp: org.apache.spark.sql.Column,
                                dewp: org.apache.spark.sql.Column) = {
    when(temp.isNotNull && dewp.isNotNull,
      100 * exp((17.625 * dewp) / (243.04 + dewp)) / exp((17.625 * temp) / (243.04 + temp))
    ).otherwise(null)
  }

  /**
   * Parse weather phenomena from FRSHTT string
   * @param frshtt FRSHTT code string
   * @return Parsed weather phenomena description
   */
  private def parseWeatherPhenomena(frshtt: org.apache.spark.sql.Column) = {
    when(frshtt.isNotNull,
      concat_ws(", ",
        when(substring(frshtt, 1, 1) === "1", lit("Fog")).otherwise(lit("")),
        when(substring(frshtt, 2, 1) === "1", lit("Rain")).otherwise(lit("")),
        when(substring(frshtt, 3, 1) === "1", lit("Snow")).otherwise(lit("")),
        when(substring(frshtt, 4, 1) === "1", lit("Hail")).otherwise(lit("")),
        when(substring(frshtt, 5, 1) === "1", lit("Thunder")).otherwise(lit("")),
        when(substring(frshtt, 6, 1) === "1", lit("Tornado")).otherwise(lit(""))
      )
    ).otherwise(null)
  }

  /**
   * Get statistics about weather data
   * @param df Weather DataFrame
   * @return Map containing weather statistics
   */
  override def getDataStatistics(df: DataFrame): Map[String, Any] = {
    val totalObservations = df.count()
    val uniqueStations = df.select("station").distinct().count()

    val tempStats = df.agg(
      avg("temperature").alias("avg_temp"),
      min("temperature").alias("min_temp"),
      max("temperature").alias("max_temp")
    ).collect()(0)

    val pressureStats = df.agg(
      avg("pressure").alias("avg_pressure"),
      min("pressure").alias("min_pressure"),
      max("pressure").alias("max_pressure")
    ).collect()(0)

    Map(
      "total_observations" -> totalObservations,
      "unique_stations" -> uniqueStations,
      "avg_temperature" -> Option(tempStats.getAs[Double]("avg_temp")).getOrElse(0.0),
      "min_temperature" -> Option(tempStats.getAs[Double]("min_temp")).getOrElse(0.0),
      "max_temperature" -> Option(tempStats.getAs[Double]("max_temp")).getOrElse(0.0),
      "avg_pressure" -> Option(pressureStats.getAs[Double]("avg_pressure")).getOrElse(0.0),
      "min_pressure" -> Option(pressureStats.getAs[Double]("min_pressure")).getOrElse(0.0),
      "max_pressure" -> Option(pressureStats.getAs[Double]("max_pressure")).getOrElse(0.0),
      "data_completeness" -> calculateDataCompleteness(df)
    )
  }

  /**
   * Calculate data completeness percentage for key weather variables
   * @param df Weather DataFrame
   * @return Data completeness percentage
   */
  private def calculateDataCompleteness(df: DataFrame): Double = {
    val totalRows = df.count().toDouble
    if (totalRows == 0) return 0.0

    val completenessMetrics = df.agg(
      (count("temperature") / totalRows * 100).alias("temp_completeness"),
      (count("pressure") / totalRows * 100).alias("pressure_completeness"),
      (count("visibility") / totalRows * 100).alias("visibility_completeness"),
      (count("wind_speed") / totalRows * 100).alias("wind_completeness")
    ).collect()(0)

    // Average completeness across key variables
    List(
      completenessMetrics.getAs[Double]("temp_completeness"),
      completenessMetrics.getAs[Double]("pressure_completeness"),
      completenessMetrics.getAs[Double]("visibility_completeness"),
      completenessMetrics.getAs[Double]("wind_completeness")
    ).sum / 4.0
  }
}
