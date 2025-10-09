package com.flightdelay.data.loaders

import com.flightdelay.config.AppConfiguration
import com.flightdelay.data.models.Flight
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Data loader specifically designed for Weather data
 * Handles loading and preprocessing of Weather information including:
 * - ...
 * - ..
 */
object WeatherDataLoader extends DataLoader[Flight] {

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
    StructField("CANCELLED", IntegerType, nullable = true),
    StructField("DIVERTED", IntegerType, nullable = true),
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
    val filePath = configuration.common.data.flight.path
    val outputPath = s"${configuration.common.output.basePath}/common/data/raw_flights.parquet"
    loadFromFilePath(filePath, validate, Some(outputPath))
  }


  /**
   * Load flight data with full preprocessing and transformation
   * @param filePath Path to CSV input file
   * @param validate Whether to validate schema
   * @param outputPath Optional path to save Parquet file
   * @param spark Implicit SparkSession
   * @return DataFrame containing processed flight data
   */
  override def loadFromFilePath(filePath: String, validate: Boolean = false, outputPath: Option[String] = None)(implicit spark: SparkSession): DataFrame = {
    println("\n" + "=" * 80)
    println("[STEP 1][DataLoader] Flight Data Loading - Start")
    println("=" * 80)

    // Check if Parquet file exists and load from it if available
    val rawDf = outputPath match {
      case Some(parquetPath) if parquetFileExists(parquetPath) =>
        println(s"\nLoading from existing Parquet file:")
        println(s"  - Path: $parquetPath")
        val df = spark.read.parquet(parquetPath)
        val count = df.count
        println(s"  - Loaded $count records from Parquet (optimized)")
        df

      case _ =>
        println(s"\nLoading from CSV file:")
        println(s"  - Path: $filePath")
        val df = spark.read.format("csv")
          .option("header", "true")
          .schema(expectedSchema)
          .option("timestampFormat", DEFAULT_DATE_FORMAT)
          .option("multiline", "true")
          .option("escape", "\"")
          .load(filePath)
          .withColumn("FL_DATE", to_date(col("FL_DATE"), DEFAULT_DATE_FORMAT))

        val count = df.count
        println(s"  - Loaded $count records from CSV")

        // Save as Parquet for future use
        outputPath.foreach { path =>
          println(s"\nSaving to Parquet format:")
          println(s"  - Path: $path")
          df.write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(path)
          println(s"  - Saved $count records to Parquet")
        }

        df
    }

    println("\nSchema:")
    rawDf.printSchema
    println("\nSample data (10 rows):")
    rawDf.show(10)

    if (validate && (!validateSchema(rawDf)))
      println("! Schema validation failed")

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