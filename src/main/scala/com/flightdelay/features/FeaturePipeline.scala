package com.flightdelay.features

import com.flightdelay.config.{AppConfiguration, ExperimentConfig}
import com.flightdelay.features.balancer.DelayBalancedDatasetBuilder
import com.flightdelay.features.joiners.FlightWeatherDataJoiner
import org.apache.spark.sql.{DataFrame, SparkSession}

object FeaturePipeline {

  def execute(
    flightData: DataFrame,
    weatherData: DataFrame,
    experiment: ExperimentConfig,
  )(implicit spark: SparkSession, configuration: AppConfiguration): String = {

    val pipelineStartTime = System.currentTimeMillis()

    println("=" * 80)
    println("[FeaturePipeline] Data Preparation Pipeline - Start")
    println("=" * 80)
    println("Note: Feature extraction will be done after train/test split to avoid data leakage")
    println("=" * 80)

    // Balance Flight Dataset
    val labeledFlightData =  DelayBalancedDatasetBuilder.prepareLabeledDataset(
      df = flightData,
      dxCol = experiment.featureExtraction.dxCol,
      delayThresholdMin = experiment.featureExtraction.delayThresholdMin,
      filterOnDxEquals1 = false  // Keep both delayed and on-time flights
    )

    // Conditional: Join and explode only if weather data is needed
    val weatherOriginDepthHours = experiment.featureExtraction.weatherOriginDepthHours
    val weatherDestinationDepthHours = experiment.featureExtraction.weatherDestinationDepthHours

    // Weather join is enabled if at least one depth is >= 0
    // Negative values explicitly disable weather join for that airport
    val weatherJoinEnabled = weatherOriginDepthHours >= 0 || weatherDestinationDepthHours >= 0

    val dataForML = if (weatherJoinEnabled) {

        println("[Mode] Weather features enabled - performing join and explode")
        println(s"  - Origin depth: $weatherOriginDepthHours hours ${if (weatherOriginDepthHours < 0) "(DISABLED)" else ""}")
        println(s"  - Destination depth: $weatherDestinationDepthHours hours ${if (weatherDestinationDepthHours < 0) "(DISABLED)" else ""}")

        // Jointure des données
        println("[Step 1/2] Join flight & Weather data...")
        var stepStartTime = System.currentTimeMillis()
        val joinedData = join(labeledFlightData, weatherData, experiment)
        var stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
        println(s"[Step 1/2] Join Completed in ${stepDuration}s")

        // Explosion de la jointure en données exploitable par ML
        println("[Step 2/2] Exploding Joined flight & Weather data...")
        stepStartTime = System.currentTimeMillis()
        val explodedData = explose(joinedData, experiment)
        stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
        println(s"[Step 2/2] Exploding Completed in ${stepDuration}s")

        explodedData

    } else {
        println("  Weather features disabled - using flight data only (no join, no explode)")

        // Just select flight features + target
        val flightFeaturesWithTarget = experiment.featureExtraction.flightSelectedFeatures.map { features =>
          val featureNames = features.keys.toSeq
          if (featureNames.contains(experiment.target)) {
            featureNames
          } else {
            println(s"  - Automatically adding target '${experiment.target}' to flight features")
            featureNames :+ experiment.target
          }
        }.getOrElse {
          // If no flight features specified, use all columns
          labeledFlightData.columns.toSeq
        }

        val selectedFlightData = labeledFlightData.select(flightFeaturesWithTarget.map(labeledFlightData(_)): _*)
        println(s"  - Selected ${flightFeaturesWithTarget.length} flight features")

        // Cache the data
        val cachedFlightData = selectedFlightData.cache()
        val count = cachedFlightData.count()
        println(s"  - Flight records: ${count}")

        cachedFlightData
    }

    // Save prepared data (feature extraction will be done in MLPipeline after split)
    val explodedDataPath = s"${configuration.common.output.basePath}/${experiment.name}/data/joined_exploded_data.parquet"
    println(s"[Saving] Prepared data for ML Pipeline:")
    println(s"  - Path: $explodedDataPath")
    println(s"  - Records: ${dataForML.count()}")

    dataForML.coalesce(100)
      .write
      .mode("overwrite")
      .option("compression", "zstd")
      .parquet(explodedDataPath)
    println(s"  - Saved successfully")

    val totalDuration = (System.currentTimeMillis() - pipelineStartTime) / 1000.0
    println("=" * 80)
    println(s"[FeaturePipeline] Data Preparation Pipeline - End (Total: ${totalDuration}s)")
    println("=" * 80)

    explodedDataPath
  }

  def join(
    flightData: DataFrame,
    weatherData: DataFrame,
    experimentConfig: ExperimentConfig
  )(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    // Jointure des données
    println("Joining flight and weather data...")

    val weatherOriginDepthHours = experimentConfig.featureExtraction.weatherOriginDepthHours
    val weatherDestinationDepthHours = experimentConfig.featureExtraction.weatherDestinationDepthHours

    // Check if weather join should be skipped (both negative)
    if (weatherOriginDepthHours < 0 && weatherDestinationDepthHours < 0) {
      println("⚠️  Both weather depth values are negative - NO weather join, returning flight data only")
      return flightData
    }

    val stepStartTime = System.currentTimeMillis()

    // Add target column to flight features if not already present
    val flightFeaturesWithTarget = experimentConfig.featureExtraction.flightSelectedFeatures.map { features =>
      val featureNames = features.keys.toSeq
      if (featureNames.contains(experimentConfig.target)) {
        featureNames
      } else {
        println(s"  - Automatically adding target '${experimentConfig.target}' to flight features")
        featureNames :+ experimentConfig.target
      }
    }

    val joinedData = FlightWeatherDataJoiner.joinFlightsWithWeather(
      flightData,
      weatherData,
      weatherOriginDepthHours= experimentConfig.featureExtraction.weatherOriginDepthHours,
      weatherDestinationDepthHours= experimentConfig.featureExtraction.weatherDestinationDepthHours,
      removeLeakageColumns = true,
      flightFeaturesWithTarget,
      experimentConfig.featureExtraction.weatherSelectedFeatures.map(_.keys.toSeq))

    // OPTIMIZATION: Cache joined data to avoid recomputation
    // This is critical since we'll need it for count, explode, and potentially save
    println("  - Caching joined data...")
    val cachedJoinedData = joinedData.cache()

    // Force materialization with a single count
    val joinedCount = cachedJoinedData.count()
    println(f"  - Joined records: ${joinedCount}%,d with ${cachedJoinedData.columns.length}%3d columns")

    if (experimentConfig.featureExtraction.storeJoinData) {
      val joinedParquetPath = s"${configuration.common.output.basePath}/${experimentConfig.name}/data/joined_flights_weather.parquet"
      println(s"Saving joined flight weather data to parquet:")
      println(s"  - Path: $joinedParquetPath")

      // Coalesce to reduce number of output files (improves write performance)
      cachedJoinedData.coalesce(100)
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

    val weatherOriginDepthHours = experimentConfig.featureExtraction.weatherOriginDepthHours
    val weatherDestinationDepthHours = experimentConfig.featureExtraction.weatherDestinationDepthHours

    // Check if both are negative (no weather data at all)
    if (weatherOriginDepthHours < 0 && weatherDestinationDepthHours < 0) {
      println("⚠️  Both weather depth values are negative - NO weather explosion needed")
      return data
    }

    // Get weather feature names from config, or auto-detect from schema
    val weatherFeatures: Seq[String] = experimentConfig.featureExtraction.weatherSelectedFeatures match {
      case Some(featuresMap) =>
        // Extract keys from the Map[String, FeatureTransformationConfig]
        featuresMap.keys.toSeq
      case None =>
        // Auto-detect: extract all field names from origin_weather_observations array struct
        if (data.columns.contains("origin_weather_observations")) {
          import org.apache.spark.sql.types.{ArrayType, StructType}
          val arraySchema = data.schema("origin_weather_observations").dataType
          arraySchema match {
            case ArrayType(elementType: StructType, _) =>
              val fields = elementType.fieldNames.toSeq
              println(s"[Auto-detect] No weatherSelectedFeatures defined, using all ${fields.length} fields from schema:")
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

    println(s"Exploding weather observation arrays:")
    println(s"  - Weather features: ${weatherFeatures.mkString(", ")}")
    println(s"  - Depth Origin hours: $weatherOriginDepthHours observations ${if (weatherOriginDepthHours < 0) "(DISABLED)" else ""}")
    println(s"  - Depth Destination hours: $weatherDestinationDepthHours observations ${if (weatherDestinationDepthHours < 0) "(DISABLED)" else ""}")
    println(s"  - Input columns: ${data.columns.length}")

    var result = data
    var totalAddedColumns = 0

    // Explode origin_weather_observations (only if depth >= 0 and column exists)
    // Pattern: origin_weather_SkyCondition-3, origin_weather_Visibility-3, ..., origin_weather_SkyCondition-0, origin_weather_Visibility-0
    // Mapping: array[0] (oldest) → suffix -N, array[N] (most recent) → suffix -0
    // Example: depth=3 → 4 observations [0, 1, 2, 3] = heure départ, départ-1, départ-2, départ-3
    if (weatherOriginDepthHours >= 0 && data.columns.contains("origin_weather_observations")) {
      (0 to weatherOriginDepthHours).foreach { arrayIdx =>
        val suffixIdx = weatherOriginDepthHours - arrayIdx  // Reverse: array[0]→-N, array[N]→-0
        weatherFeatures.foreach { feature =>
          result = result.withColumn(
            s"origin_weather_${feature}-${suffixIdx}",
            col("origin_weather_observations").getItem(arrayIdx).getField(feature)
          )
          totalAddedColumns += 1
        }
      }
      result = result.drop("origin_weather_observations")
      val numObs = weatherOriginDepthHours + 1
      println(s"  - Exploded origin_weather_observations into ${numObs * weatherFeatures.length} columns ($numObs observations)")
    } else if (weatherOriginDepthHours < 0) {
      println(s"  - Skipped origin_weather_observations explosion (disabled)")
    }

    // Explode destination_weather_observations (only if depth >= 0 and column exists)
    // Pattern: destination_weather_SkyCondition-3, destination_weather_Visibility-3, ..., destination_weather_SkyCondition-0, destination_weather_Visibility-0
    // Mapping: array[0] (oldest) → suffix -N, array[N] (most recent) → suffix -0
    // Example: depth=3 → 4 observations [0, 1, 2, 3] = heure arrivée, arrivée-1, arrivée-2, arrivée-3
    if (weatherDestinationDepthHours >= 0 && data.columns.contains("destination_weather_observations")) {
      (0 to weatherDestinationDepthHours).foreach { arrayIdx =>
        val suffixIdx = weatherDestinationDepthHours - arrayIdx  // Reverse: array[0]→-N, array[N]→-0
        weatherFeatures.foreach { feature =>
          result = result.withColumn(
            s"destination_weather_${feature}-${suffixIdx}",
            col("destination_weather_observations").getItem(arrayIdx).getField(feature)
          )
          totalAddedColumns += 1
        }
      }
      result = result.drop("destination_weather_observations")
      val numObs = weatherDestinationDepthHours + 1
      println(s"  - Exploded destination_weather_observations into ${numObs * weatherFeatures.length} columns ($numObs observations)")
    } else if (weatherDestinationDepthHours < 0) {
      println(s"  - Skipped destination_weather_observations explosion (disabled)")
    }

    // Cleanup: supprimer les colonnes weather_observations restantes si les valeurs sont négatives
    if (weatherOriginDepthHours < 0 && result.columns.contains("origin_weather_observations")) {
      println(s"  - Removing origin_weather_observations (depth=$weatherOriginDepthHours)")
      result = result.drop("origin_weather_observations")
    }

    if (weatherDestinationDepthHours < 0 && result.columns.contains("destination_weather_observations")) {
      println(s"  - Removing destination_weather_observations (depth=$weatherDestinationDepthHours)")
      result = result.drop("destination_weather_observations")
    }

    println(s"  - Total added columns: ${totalAddedColumns}")
    println(s"  - Output columns: ${result.columns.length}")

    // OPTIMIZATION: Cache exploded data before any action (count or save)
    println("  - Caching exploded data...")
    val cachedResult = result.cache()

    // Optionally save exploded data
    if (experimentConfig.featureExtraction.storeExplodeJoinData) {
      val explodedParquetPath = s"${configuration.common.output.basePath}/${experimentConfig.name}/data/joined_exploded_data.parquet"
      println(s"Saving exploded data to parquet:")
      println(s"  - Path: $explodedParquetPath")

      // Force materialization with count before save
      val explodedCount = cachedResult.count()
      println(s"  - Exploded records: ${explodedCount}")

      // Coalesce to reduce number of output files
      cachedResult.coalesce(100)
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
}
