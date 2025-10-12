package com.flightdelay.data.loaders

import com.flightdelay.config.AppConfiguration
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

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
    try {
      loadFromFilePath(filePath, validate, Some(outputPath))
    } catch {
      case e: org.apache.spark.sql.AnalysisException if e.getMessage.contains("PATH_NOT_FOUND") =>
        println(s"Warning: WBAN-Airport-Timezone mapping file not found at $filePath. Skipping mapping loading.")
        // Return empty DataFrame with expected schema
        import org.apache.spark.sql.Row
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], expectedSchema)
    }
  }

  /**
   * Load WBAN-Airport-Timezone mapping from file
   * @param filePath Path to CSV input file
   * @param validate Whether to validate schema
   * @param outputPath Optional path to save Parquet file
   * @param spark Implicit SparkSession
   * @return DataFrame containing WBAN-Airport-Timezone mapping
   */
  override def loadFromFilePath(filePath: String, validate: Boolean = false, outputPath: Option[String] = None)(implicit spark: SparkSession): DataFrame = {
    println("\n" + "=" * 80)
    println("[STEP 1][DataLoader] WBAN-Airport-Timezone Mapping Loading - Start")
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
          .option("multiline", "true")
          .option("escape", "\"")
          .load(filePath)

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
      "AirportID",
      "WBAN",
      "TimeZone"
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
