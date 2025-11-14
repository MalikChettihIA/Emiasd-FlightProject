package com.flightdelay.data.loaders

import com.flightdelay.config.AppConfiguration
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import com.flightdelay.utils.DebugUtils._

/**
 * Data loader for WBAN-Airport-Timezone mapping
 * Maps airport IDs to weather station IDs (WBAN) and timezones
 */
object WBANAirportTimezoneLoader extends DataLoader[Nothing] {

  // ===========================================================================================
  // SCHEMA DEFINITION
  // ===========================================================================================

  private def expectedSchema: StructType = StructType(Array(
    StructField("AirportID", IntegerType, nullable = false),
    StructField("WBAN", StringType, nullable = false),
    StructField("TimeZone", IntegerType, nullable = false)
  ))

  // ===========================================================================================
  // CORE LOADING METHODS
  // ===========================================================================================

  /**
   * Load WBAN-Airport-Timezone mapping from configuration
   * @param validate Whether to validate schema
   * @param spark Implicit SparkSession
   * @param configuration Application configuration
   * @return DataFrame containing WBAN-Airport-Timezone mapping
   */
  override def loadFromConfiguration(validate: Boolean = false)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {
    val filePath = configuration.common.data.airportMapping.path
    val outputPath = s"${configuration.common.output.basePath}/common/data/raw_wban_airport_timezone.parquet"
    loadFromFilePath(filePath, validate, Some(outputPath))
  }

  /**
   * Load WBAN-Airport-Timezone mapping from file
   * @param filePath Path to CSV input file
   * @param validate Whether to validate schema
   * @param outputPath Optional path to save Parquet file
   * @param spark Implicit SparkSession
   * @return DataFrame containing WBAN-Airport-Timezone mapping
   */
  override def loadFromFilePath(filePath: String, validate: Boolean = false, outputPath: Option[String] = None)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {
    debug("=" * 80)
    debug("[STEP 1][DataLoader] WBAN-Airport-Timezone Mapping Loading - Start")
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
      // Load from CSV file
      debug(s"Loading from CSV file:")
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
      "AirportID",
      "WBAN",
      "TimeZone"
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
