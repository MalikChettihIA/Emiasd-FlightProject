package com.flightdelay.data.loaders

import com.flightdelay.config.AppConfiguration
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import com.flightdelay.utils.DebugUtils._

/**
 * Data loader specifically designed for weather data
 * Handles loading and preprocessing of weather information including:
 * - Weather station measurements
 * - Temperature, humidity, and wind conditions
 * - Visibility and precipitation data
 * - Atmospheric pressure readings
 */
object WeatherDataLoader extends DataLoader[Nothing] {

  // ===========================================================================================
  // CONFIGURATION AND CONSTANTS
  // ===========================================================================================

  private val DEFAULT_DATE_FORMAT = "yyyyMMdd"
  private val DEFAULT_TIME_FORMAT = "HHmm"

  // ===========================================================================================
  // SCHEMA DEFINITION
  // ===========================================================================================

  private def expectedSchema: StructType = StructType(Array(
    // Identification columns
    StructField("WBAN", StringType, nullable = false),
    StructField("Date", StringType, nullable = false),
    StructField("Time", StringType, nullable = false),

    // Station and sky conditions
    StructField("StationType", IntegerType, nullable = true),
    StructField("SkyCondition", StringType, nullable = true),
    StructField("SkyConditionFlag", StringType, nullable = true),

    // Visibility
    StructField("Visibility", DoubleType, nullable = true),
    StructField("VisibilityFlag", StringType, nullable = true),

    // Weather type
    StructField("WeatherType", StringType, nullable = true),
    StructField("WeatherTypeFlag", StringType, nullable = true),

    // Temperature - Dry Bulb
    StructField("DryBulbFarenheit", DoubleType, nullable = true),
    StructField("DryBulbFarenheitFlag", StringType, nullable = true),
    StructField("DryBulbCelsius", DoubleType, nullable = true),
    StructField("DryBulbCelsiusFlag", StringType, nullable = true),

    // Temperature - Wet Bulb
    StructField("WetBulbFarenheit", DoubleType, nullable = true),
    StructField("WetBulbFarenheitFlag", StringType, nullable = true),
    StructField("WetBulbCelsius", DoubleType, nullable = true),
    StructField("WetBulbCelsiusFlag", StringType, nullable = true),

    // Temperature - Dew Point
    StructField("DewPointFarenheit", DoubleType, nullable = true),
    StructField("DewPointFarenheitFlag", StringType, nullable = true),
    StructField("DewPointCelsius", DoubleType, nullable = true),
    StructField("DewPointCelsiusFlag", StringType, nullable = true),

    // Humidity
    StructField("RelativeHumidity", DoubleType, nullable = true),
    StructField("RelativeHumidityFlag", StringType, nullable = true),

    // Wind
    StructField("WindSpeed", DoubleType, nullable = true),
    StructField("WindSpeedFlag", StringType, nullable = true),
    StructField("WindDirection", DoubleType, nullable = true),
    StructField("WindDirectionFlag", StringType, nullable = true),
    StructField("ValueForWindCharacter", StringType, nullable = true),
    StructField("ValueForWindCharacterFlag", StringType, nullable = true),

    // Pressure
    StructField("StationPressure", DoubleType, nullable = true),
    StructField("StationPressureFlag", StringType, nullable = true),
    StructField("PressureTendency", StringType, nullable = true),
    StructField("PressureTendencyFlag", StringType, nullable = true),
    StructField("PressureChange", DoubleType, nullable = true),
    StructField("PressureChangeFlag", StringType, nullable = true),
    StructField("SeaLevelPressure", StringType, nullable = true),
    StructField("SeaLevelPressureFlag", StringType, nullable = true),

    // Record type
    StructField("RecordType", StringType, nullable = true),
    StructField("RecordTypeFlag", StringType, nullable = true),

    // Precipitation
    StructField("HourlyPrecip", StringType, nullable = true),
    StructField("HourlyPrecipFlag", StringType, nullable = true),

    // Altimeter
    StructField("Altimeter", DoubleType, nullable = true),
    StructField("AltimeterFlag", StringType, nullable = true)
  ))

  // ===========================================================================================
  // CORE LOADING METHODS
  // ===========================================================================================

  /**
   * Load weather data from configuration
   * @param validate Whether to validate schema
   * @param spark Implicit SparkSession
   * @param configuration Application configuration
   * @return DataFrame containing weather data
   */
  override def loadFromConfiguration(validate: Boolean = false)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {
    val filePath = configuration.common.data.weather.path
    val outputPath = s"${configuration.common.output.basePath}/common/data/raw_weather.parquet"
    loadFromFilePath(filePath, validate, Some(outputPath))
  }

  /**
   * Load weather data with full preprocessing and transformation
   * @param filePath Path to CSV input file
   * @param validate Whether to validate schema
   * @param outputPath Optional path to save Parquet file
   * @param spark Implicit SparkSession
   * @return DataFrame containing weather data
   */
  override def loadFromFilePath(filePath: String, validate: Boolean = false, outputPath: Option[String] = None)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {
    debug("=" * 80)
    debug("[STEP 1][DataLoader] Weather Data Loading - Start")
    debug("=" * 80)

    val rawDf = if (!configuration.common.loadDataFromCSV) {
      // Load from existing Parquet file
      val parquetPath = outputPath.get
      info(s"Loading from existing Parquet file:")
      info(s"  - Path: $parquetPath")

      val df = spark.read.parquet(parquetPath)

      whenDebug {
        val count = df.count()
        println(s"  - Loaded $count records from Parquet")
      }

      df
    } else {
      // Load from TXT file
      debug(s"Loading from TXT file:")
      debug(s"  - Path: $filePath")
      val df = spark.read.format("csv")
        .option("header", "true")
        .schema(expectedSchema)
        .option("multiline", "true")
        .option("escape", "\"")
        .load(filePath)

      whenDebug {
         val count = df.count
         println(s"  - Loaded $count records from CSV")
      }

      // Save as Parquet for future use
      outputPath.foreach { path =>
        info(s"Saving to Parquet format:")
        info(s"  - Path: $path")
        df.write
          .mode("overwrite")
          .option("compression", "snappy")
          .parquet(path)
        debug(s"  - Saved records to Parquet")
      }

      df
    }

    whenDebug{
      println("Schema:")
      rawDf.printSchema
    }

    if (validate && (!validateSchema(rawDf)))
      error("! Schema validation failed")

    rawDf
  }

  /**
   * Check if Parquet file exists
   */
  private def parquetFileExists(path: String)(implicit spark: SparkSession): Boolean = {
    try {
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val exists = fs.exists(new org.apache.hadoop.fs.Path(path))
      exists
    } catch {
      case _: Exception => false
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
  private def validateSchema(df: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): Boolean = {
    val requiredColumns = Set(
      "WBAN",
      "Date",
      "Time"
    )
    val availableColumns = df.columns.toSet

    val hasRequiredColumns = requiredColumns.subsetOf(availableColumns)

    if (!hasRequiredColumns) {
      val missingColumns = requiredColumns -- availableColumns
      error(s"Missing required columns: ${missingColumns.mkString(", ")}")
    }

    hasRequiredColumns
  }

}
