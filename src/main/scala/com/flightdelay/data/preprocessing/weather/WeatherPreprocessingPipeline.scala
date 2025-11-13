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
    println(s"  - Loaded raw records")

    // Execute preprocessing pipeline (each step creates a new DataFrame)
    val processedWithSkyConditionFeatureDf = originalDf.transform(SkyConditionFeatures.createSkyConditionFeatures)
    val porcessedWithVisibilityFeaturesDf = processedWithSkyConditionFeatureDf.transform(VisibilityFeatures.createVisibilityFeatures)
    val porcessedWithSkyConditionAndVisibilityIntegrationFeaturesDf = porcessedWithVisibilityFeaturesDf.transform(WeatherInteractionFeatures.createInteractionFeatures)
    val porcessWithWeatherConditionFeaturesDf = porcessedWithSkyConditionAndVisibilityIntegrationFeaturesDf.transform(WeatherTypeFeatureGenerator.createFeatures)
    val processedWithPressureTendencyFeaturesDf = porcessWithWeatherConditionFeaturesDf.transform(WeatherPressionFeatures.createPressureFeatures)
    val processedWeatherDf = WeatherDataCleaner.preprocess(processedWithPressureTendencyFeaturesDf)

    // ⭐ PERSIST to disk before write to break DAG and prevent OOM
    println("\n[Pipeline Step 8/8] Persisting to disk before write to break DAG...")
    import org.apache.spark.storage.StorageLevel
    val persistedData = processedWeatherDf.persist(StorageLevel.DISK_ONLY)

    // Force persist materialization (but data stays on disk, not in memory)
    val recordCount = persistedData.count()
    println(s"  ✓ Persisted ${recordCount} records to disk")
    logMemoryUsage("After persist")

    // ⭐ OPTIMIZED WRITE STRATEGY
    writeWeatherDataSafely(persistedData, processedParquetPath)

    // Cleanup persisted data
    persistedData.unpersist()

    println("\n" + "=" * 80)
    println("[Preprocessing] Weather Data Preprocessing Pipeline - End")
    println("=" * 80 + "\n")

    // Relire les données sauvegardées pour retourner un DataFrame propre
    spark.read.parquet(processedParquetPath)
  }

  /**
   * Stratégie d'écriture sécurisée pour éviter les OOM sur les données météo
   */
  private def writeWeatherDataSafely(
                                      weatherDf: DataFrame,
                                      outputPath: String
                                    )(implicit spark: SparkSession): Unit = {

    import spark.implicits._

    println(s"\nSaving preprocessed weather data to parquet:")
    println(s"  - Path: $outputPath")
    println("  - Using optimized write strategy")

    // ⭐ WRITE directement sans partitionBy pour minimiser la mémoire
    println("\n  [Step 1/2] Writing to parquet (using existing partitions)...")
    try {
      weatherDf
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(outputPath)

      println("  ✓ Write completed successfully")

    } catch {
      case e: Exception =>
        println(s"  ✗ Write failed: ${e.getMessage}")
        println("\n  Attempting fallback: batch write strategy...")

        writeWeatherInBatches(weatherDf, outputPath)
    }

    println(s"\n  ✓ Weather data saved successfully")
  }

  /**
   * Fallback: écriture par batch pour les données météo
   */
  private def writeWeatherInBatches(
                                     weatherDf: DataFrame,
                                     outputPath: String,
                                     batchSize: Int = 2000000  // 2M rows per batch
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