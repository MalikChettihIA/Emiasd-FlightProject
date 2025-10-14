package com.flightdelay.features

import com.flightdelay.config.{AppConfiguration, ExperimentConfig}
import com.flightdelay.data.loaders.FlightDataLoader
import com.flightdelay.features.joiners.FlightWeatherDataJoiner
import org.apache.spark.sql.{DataFrame, SparkSession}

object FeaturePipeline {

  def execute(
    flightData: DataFrame,
    weatherData: DataFrame,
    experiment: ExperimentConfig,
  )(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    val pipelineStartTime = System.currentTimeMillis()

    println("\n" + "=" * 80)
    println("[FeaturePipeline] Feature Extraction Pipeline - Start")
    println("=" * 80)

    // Jointure des données
    println("\n[Step 1/3] Join flight & Weather data...")
    var stepStartTime = System.currentTimeMillis()
    val joinedData = join(flightData, weatherData, experiment)
    var stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    println(s"[Step 1/3] Join Completed in ${stepDuration}s")

    // Explosion de la jointure en données exploitable par ML
    println("\n[Step 2/3] Exploding Joined flight & Weather data...")
    stepStartTime = System.currentTimeMillis()
    val explosedData = explose(joinedData, experiment)
    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    println(s"[Step 2/3] Exploding Completed in ${stepDuration}s")

    // Extraction des features
    println("\n[Step 3/3] Extracting features from Joined flight & Weather data...")
    stepStartTime = System.currentTimeMillis()
    val extractedData = extractFeature(explosedData, experiment)
    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    println(s"[Step 3/3] Feature Extraction Completed in ${stepDuration}s")

    val totalDuration = (System.currentTimeMillis() - pipelineStartTime) / 1000.0
    println("\n" + "=" * 80)
    println(s"[FeaturePipeline] Feature Extraction Pipeline - End (Total: ${totalDuration}s)")
    println("=" * 80 + "\n")

    extractedData
  }

  def join(
    flightData: DataFrame,
    weatherData: DataFrame,
    experimentConfig: ExperimentConfig
  )(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    // Jointure des données
    println("\nJoining flight and weather data...")

    val stepStartTime = System.currentTimeMillis()
    val joinedData = FlightWeatherDataJoiner.joinFlightsWithWeather(
      flightData,
      weatherData,
      weatherDepthHours = experimentConfig.featureExtraction.weatherDepthHours,
      removeLeakageColumns = true,
      experimentConfig.featureExtraction.flightSelectedFeatures,
      experimentConfig.featureExtraction.weatherSelectedFeatures)

    // OPTIMIZATION: Cache joined data to avoid recomputation
    // This is critical since we'll need it for count, explode, and potentially save
    println("  - Caching joined data...")
    val cachedJoinedData = joinedData.cache()

    // Force materialization with a single count
    val joinedCount = cachedJoinedData.count()
    println(f"  - Joined records: ${joinedCount}%,d with ${cachedJoinedData.columns.length}%3d columns")

    if (experimentConfig.featureExtraction.storeJoinData) {
      val joinedParquetPath = s"${configuration.common.output.basePath}/${experimentConfig.name}/data/joined_flights_weather.parquet"
      println(s"\nSaving joined flight weather data to parquet:")
      println(s"  - Path: $joinedParquetPath")

      // Coalesce to reduce number of output files (improves write performance)
      cachedJoinedData.coalesce(8)
        .write
        .mode("overwrite")
        .option("compression", "zstd")  // Better compression than snappy
        .parquet(joinedParquetPath)
      println(s"  - Saved ${joinedCount} joined records (already counted)")
    }

    cachedJoinedData
  }

  def explose(data: DataFrame, experimentConfig: ExperimentConfig)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    import org.apache.spark.sql.functions._

    // Get weather feature names from config, or auto-detect from schema
    val weatherFeatures = experimentConfig.featureExtraction.weatherSelectedFeatures.getOrElse {
      // Auto-detect: extract all field names from origin_weather_observations array struct
      if (data.columns.contains("origin_weather_observations")) {
        import org.apache.spark.sql.types.{ArrayType, StructType}
        val arraySchema = data.schema("origin_weather_observations").dataType
        arraySchema match {
          case ArrayType(elementType: StructType, _) =>
            val fields = elementType.fieldNames.toSeq
            println(s"\n[Auto-detect] No weatherSelectedFeatures defined, using all ${fields.length} fields from schema:")
            println(s"  ${fields.mkString(", ")}")
            fields
          case _ =>
            throw new IllegalArgumentException(
              "weatherSelectedFeatures not defined and cannot auto-detect from origin_weather_observations schema"
            )
        }
      } else {
        throw new IllegalArgumentException(
          "weatherSelectedFeatures must be defined in configuration when origin_weather_observations column is missing"
        )
      }
    }

    val weatherDepthHours = experimentConfig.featureExtraction.weatherDepthHours

    println(s"\nExploding weather observation arrays:")
    println(s"  - Weather features: ${weatherFeatures.mkString(", ")}")
    println(s"  - Depth hours: $weatherDepthHours observations")
    println(s"  - Input columns: ${data.columns.length}")

    var result = data
    var totalAddedColumns = 0

    // Explode origin_weather_observations
    // Pattern: origin_weather_SkyCondition-11, origin_weather_Visibility-11, ..., origin_weather_SkyCondition-0, origin_weather_Visibility-0
    // Mapping: array[0] (oldest) → suffix -11, array[11] (most recent) → suffix -0
    if (data.columns.contains("origin_weather_observations")) {
      (0 until weatherDepthHours).foreach { arrayIdx =>
        val suffixIdx = weatherDepthHours - 1 - arrayIdx  // Reverse: array[0]→-11, array[11]→-0
        weatherFeatures.foreach { feature =>
          result = result.withColumn(
            s"origin_weather_${feature}-${suffixIdx}",
            col("origin_weather_observations").getItem(arrayIdx).getField(feature)
          )
          totalAddedColumns += 1
        }
      }
      result = result.drop("origin_weather_observations")
      println(s"  - Exploded origin_weather_observations into ${weatherDepthHours * weatherFeatures.length} columns")
    }

    // Explode destination_weather_observations
    // Pattern: destination_weather_SkyCondition-11, destination_weather_Visibility-11, ..., destination_weather_SkyCondition-0, destination_weather_Visibility-0
    if (data.columns.contains("destination_weather_observations")) {
      (0 until weatherDepthHours).foreach { arrayIdx =>
        val suffixIdx = weatherDepthHours - 1 - arrayIdx  // Reverse: array[0]→-11, array[11]→-0
        weatherFeatures.foreach { feature =>
          result = result.withColumn(
            s"destination_weather_${feature}-${suffixIdx}",
            col("destination_weather_observations").getItem(arrayIdx).getField(feature)
          )
          totalAddedColumns += 1
        }
      }
      result = result.drop("destination_weather_observations")
      println(s"  - Exploded destination_weather_observations into ${weatherDepthHours * weatherFeatures.length} columns")
    }

    println(s"  - Total added columns: ${totalAddedColumns}")
    println(s"  - Output columns: ${result.columns.length}")
    println(s"  - Column organization: grouped by index (${weatherDepthHours-1} to 0)")

    // OPTIMIZATION: Cache exploded data before any action (count or save)
    println("  - Caching exploded data...")
    val cachedResult = result.cache()

    // Optionally save exploded data
    if (experimentConfig.featureExtraction.storeExplodeJoinData) {
      val explodedParquetPath = s"${configuration.common.output.basePath}/${experimentConfig.name}/data/exploded_joined_data.parquet"
      println(s"\nSaving exploded data to parquet:")
      println(s"  - Path: $explodedParquetPath")

      // Force materialization with count before save
      val explodedCount = cachedResult.count()
      println(s"  - Exploded records: ${explodedCount}")

      // Coalesce to reduce number of output files
      cachedResult.coalesce(8)
        .write
        .mode("overwrite")
        .option("compression", "zstd")  // Better compression
        .parquet(explodedParquetPath)
      println(s"  - Saved ${explodedCount} exploded records")
    } else {
      // Force materialization even if we don't save
      // This ensures the cache is populated before feature extraction
      val explodedCount = cachedResult.count()
      println(s"  - Exploded records: ${explodedCount}")
    }

    cachedResult
  }

  def extractFeature(data: DataFrame, experimentConfig: ExperimentConfig)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {
    val extractedData = FeatureExtractor.extract(data, experimentConfig)
    extractedData
  }
}
