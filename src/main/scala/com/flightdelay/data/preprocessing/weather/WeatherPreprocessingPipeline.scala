package com.flightdelay.data.preprocessing.weather

import com.flightdelay.config.AppConfiguration
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object WeatherPreprocessingPipeline {

  /**
   * Exécute le pipeline complet de preprocessing pour les données météo
   * Charge les données depuis le fichier parquet généré par WeatherDataLoader
   * @param configuration Configuration contenant les paramètres de chargement
   * @param spark Session Spark
   * @return DataFrame complètement préprocessé
   */
  def execute()(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    println("\n" + "=" * 80)
    println("[Preprocessing] Weather Data Preprocessing Pipeline - Start")
    println("=" * 80)

    val processedParquetPath = s"${configuration.common.output.basePath}/common/data/processed_weather.parquet"

    // Load raw data from parquet
    val rawParquetPath = s"${configuration.common.output.basePath}/common/data/raw_weather.parquet"
    println(s"\nLoading raw data from parquet:")
    println(s"  - Path: $rawParquetPath")
    val originalDf = spark.read.parquet(rawParquetPath)
    println(s"  - Loaded ${originalDf.count()} raw records")

    // Execute preprocessing pipeline (each step creates a new DataFrame)
    val processedWithSkyConditionFeatureDf = originalDf.transform(SkyConditionFeatures.createSkyConditionFeatures)
    val porcessedWithVisibilityFeaturesDf = processedWithSkyConditionFeatureDf.transform(VisibilityFeatures.createVisibilityFeatures)
    val porcessedWithSkyConditionAndVisibilityIntegrationFeaturesDf = porcessedWithVisibilityFeaturesDf.transform(WeatherInteractionFeatures.createInteractionFeatures)
    val porcessWithWeatherConditionFeaturesDf = porcessedWithSkyConditionAndVisibilityIntegrationFeaturesDf.transform(WeatherTypeFeatureGenerator.createFeatures)
    val processedWeatherDf = WeatherDataCleaner.preprocess(porcessWithWeatherConditionFeaturesDf)

    // ⭐ OPTIMIZED WRITE STRATEGY TO AVOID OOM
    writeWeatherDataSafely(processedWeatherDf, processedParquetPath)

    println("\n" + "=" * 80)
    println("[Preprocessing] Weather Data Preprocessing Pipeline - End")
    println("=" * 80 + "\n")

    // Relire les données sauvegardées pour retourner un DataFrame propre
    spark.read.parquet(processedParquetPath)
  }

  /**
   * Stratégie d'écriture sécurisée pour éviter les OOM
   *
   * Optimisations:
   * 1. Checkpoint avant write pour libérer le DAG
   * 2. Partitionnement par Date pour écriture incrémentale
   * 3. Compression Snappy (plus légère que zstd)
   * 4. Monitoring mémoire
   */
  private def writeWeatherDataSafely(
                                      weatherDf: DataFrame,
                                      outputPath: String
                                    )(implicit spark: SparkSession): Unit = {

    import spark.implicits._

    println(s"\nSaving preprocessed data to parquet:")
    println(s"  - Path: $outputPath")
    println("  - Using optimized write strategy to avoid OOM")

    // Étape 1 : Monitoring mémoire initial
    logMemoryUsage("Before write")

    val rowCount = weatherDf.count()
    println(s"  - Rows to write: ${rowCount}")

    // Étape 2 : Checkpoint pour matérialiser et libérer le DAG
    println("\n  [Step 1/3] Checkpointing data to break DAG lineage...")
    val checkpointed = weatherDf
      .repartition(100, col("Date"))  // Partitionner par date avec 100 partitions
      .checkpoint()

    // Force l'exécution du checkpoint
    val checkpointedCount = checkpointed.count()
    println(s"  ✓ Checkpoint completed: ${checkpointedCount} rows")

    logMemoryUsage("After checkpoint")

    // Étape 3 : Write avec partitionnement par Date
    println("\n  [Step 2/3] Writing to parquet with date partitioning...")
    try {
      checkpointed
        .write
        .mode("overwrite")
        .partitionBy("Date")                    // ⭐ Partitionnement par date
        .option("compression", "snappy")        // ⭐ Snappy au lieu de zstd (moins gourmand)
        .option("maxRecordsPerFile", 500000)    // Limite records par fichier
        .parquet(outputPath)

      println("  ✓ Write completed successfully")

    } catch {
      case e: Exception =>
        println(s"  ✗ Write failed: ${e.getMessage}")
        println("\n  Attempting fallback: batch write strategy...")

        // Fallback : Write par batch si le write normal échoue
        writeInBatches(checkpointed, outputPath)
    }

    // Étape 4 : Cleanup
    println("\n  [Step 3/3] Cleaning up...")
    checkpointed.unpersist()
    System.gc()

    logMemoryUsage("After write")

    println(s"\n  ✓ Weather data saved successfully")
  }

  /**
   * Stratégie de fallback : écriture par batch
   * Utilisée si l'écriture normale échoue
   */
  private def writeInBatches(
                              weatherDf: DataFrame,
                              outputPath: String,
                              batchSize: Int = 2000000
                            )(implicit spark: SparkSession): Unit = {

    import spark.implicits._

    val totalRows = weatherDf.count()
    val numBatches = Math.ceil(totalRows.toDouble / batchSize).toInt

    println(s"  Writing in ${numBatches} batches of ${batchSize} rows...")

    // Ajouter batch_num pour partitionner
    val withBatchNum = weatherDf
      .withColumn("temp_row_id", monotonically_increasing_id())
      .withColumn("temp_batch_num", (col("temp_row_id") / batchSize).cast("int"))
      .cache()

    withBatchNum.count()  // Matérialise

    // Écrire chaque batch
    (0 until numBatches).foreach { batchNum =>
      print(s"  Batch ${batchNum + 1}/${numBatches}... ")

      val batchData = withBatchNum
        .filter(col("temp_batch_num") === batchNum)
        .drop("temp_row_id", "temp_batch_num")

      batchData
        .coalesce(5)
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(s"${outputPath}/batch_${batchNum}")

      println("✓")

      // GC tous les 2 batches
      if (batchNum % 2 == 1) {
        System.gc()
        Thread.sleep(300)
      }
    }

    withBatchNum.unpersist()

    println(s"  ✓ All ${numBatches} batches written")
    println(s"  ℹ️  Data can be read with: spark.read.parquet('${outputPath}/batch_*')")
  }

  /**
   * Monitoring mémoire
   */
  private def logMemoryUsage(label: String): Unit = {
    val runtime = Runtime.getRuntime
    val maxMemory = runtime.maxMemory() / (1024 * 1024 * 1024)
    val totalMemory = runtime.totalMemory() / (1024 * 1024 * 1024)
    val freeMemory = runtime.freeMemory() / (1024 * 1024 * 1024)
    val usedMemory = totalMemory - freeMemory

    val usagePercent = ((usedMemory / maxMemory) * 100).toInt

    println(f"  [$label] Memory: ${usedMemory}%.2f GB / ${maxMemory}%.2f GB (${usagePercent}%%)")

    if (usagePercent > 85) {
      println(s"  ⚠️  WARNING: Memory usage at ${usagePercent}% - forcing GC...")
      System.gc()
      Thread.sleep(1000)
    }
  }
}