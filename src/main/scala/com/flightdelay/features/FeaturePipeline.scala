package com.flightdelay.features

import com.flightdelay.config.{AppConfiguration, ExperimentConfig}
import com.flightdelay.features.balancer.DelayBalancedDatasetBuilder
import com.flightdelay.features.joiners.FlightWeatherDataJoiner
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.flightdelay.utils.DebugUtils._
import com.flightdelay.utils.MetricsUtils

object FeaturePipeline {

  def execute(
    flightData: DataFrame,
    weatherData: DataFrame,
    experiment: ExperimentConfig,
  )(implicit spark: SparkSession, configuration: AppConfiguration): (DataFrame, DataFrame) = {

    val pipelineStartTime = System.currentTimeMillis()

    info("=" * 80)
    info("[FeaturePipeline] Data Preparation Pipeline - Start")
    info("=" * 80)
    info("STRATEGY: Split train/test BEFORE join/explosion for:")
    info("  1. Guaranteed balanced train/test (50/50 each)")
    info("  2. Smaller datasets during join/explosion")
    info("  3. No data leakage (split happens early)")
    info("=" * 80)

    // Step 1: Label flights (add is_delayed column)
    info("[FeaturePipeline][Step 1/5] Labeling flights with is_delayed")
    val labeledFlightData =  DelayBalancedDatasetBuilder.prepareLabeledDataset(
      df = flightData,
      dxCol = experiment.featureExtraction.dxCol
    )

    // Step 2: Sample flights according to nDelayed/nOnTime from configuration
    info("=" * 80)
    info("[FeaturePipeline][Step 2/5] Sampling flights based on configuration")
    info("=" * 80)
    val (balancedFlightTrainData, balancedFlightTestData) = DelayBalancedDatasetBuilder.buildBalancedTrainTest(
      labeledDf = labeledFlightData,
      seed = configuration.common.seed
    )


    // Step 4 & 5: Process train and test separately (join + explode)
    info("=" * 80)
    info("[FeaturePipeline][Step 4/5] Processing TRAIN flights (join + explode)")
    info("=" * 80)
    val trainData = processFlightDataset(balancedFlightTrainData, weatherData, experiment, "TRAIN")
    info("[FeaturePipeline][Step 4/5] Processing TRAIN flights (count)"+ trainData.count())

    info("=" * 80)
    info("[FeaturePipeline][Step 5/5] Processing TEST flights (join + explode)")
    info("=" * 80)
    val testData = processFlightDataset(balancedFlightTestData, weatherData, experiment, "TEST")
    info("[FeaturePipeline][Step 5/5] Processing TEST flights (count)"+ testData.count())

    // Optional: Save processed data
    if (configuration.common.storeIntoParquet) {

      if (experiment.featureExtraction.storeJoinData) {
        val trainPath = s"${configuration.common.output.basePath}/${experiment.name}/data/join_exploded_train_prepared.parquet"
        info(s"Saving joined flight weather data to parquet:")
        info(s"  - Path: $trainPath")

        // Coalesce to reduce number of output files (improves write performance)
        trainData.coalesce(100)
          .write
          .mode("overwrite")
          .option("compression", "zstd")  // Better compression than snappy
          .parquet(trainPath)

        val testPath = s"${configuration.common.output.basePath}/${experiment.name}/data/join_exploded_test_prepared.parquet"
        info(s"Saving joined flight weather data to parquet:")
        info(s"  - Path: $testPath")

        // Coalesce to reduce number of output files (improves write performance)
        testData.coalesce(100)
          .write
          .mode("overwrite")
          .option("compression", "zstd")  // Better compression than snappy
          .parquet(testPath)

        info(s"  - Saved successfully")
      }


    }

    val totalDuration = (System.currentTimeMillis() - pipelineStartTime) / 1000.0
    info("=" * 80)
    info(s"[FeaturePipeline] Data Preparation Pipeline - End (Total: ${totalDuration}s)")
    info("=" * 80)

    (trainData, testData)
  }

  /**
   * Process a single flight dataset (join + explode or select features)
   * Used for both train and test to avoid code duplication
   */
  private def processFlightDataset(
    flightData: DataFrame,
    weatherData: DataFrame,
    experiment: ExperimentConfig,
    datasetName: String
  )(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    val weatherOriginDepthHours = experiment.featureExtraction.weatherOriginDepthHours
    val weatherDestinationDepthHours = experiment.featureExtraction.weatherDestinationDepthHours
    val weatherJoinEnabled = weatherOriginDepthHours >= 0 || weatherDestinationDepthHours >= 0

    if (weatherJoinEnabled) {
      info(s"[$datasetName] Weather features enabled - performing join and explode")
      info(s"  - Origin depth: $weatherOriginDepthHours hours ${if (weatherOriginDepthHours < 0) "(DISABLED)" else ""}")
      info(s"  - Destination depth: $weatherDestinationDepthHours hours ${if (weatherDestinationDepthHours < 0) "(DISABLED)" else ""}")

      // Join
      var stepStartTime = System.currentTimeMillis()
      val joinedData = join(flightData, weatherData, experiment).cache()
      var stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
      debug(s"[$datasetName] Join completed in ${stepDuration}s - ${joinedData.count()} lines")

      // Explode
      stepStartTime = System.currentTimeMillis()
      val explodedData = explose(joinedData, experiment).cache()
      stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
      debug(s"[$datasetName] Explode completed in ${stepDuration}s - ${explodedData.count()} lines")

      explodedData

    } else {
      info(s"[$datasetName] Weather features disabled - using flight data only")

      // Select flight features + target
      val flightFeaturesWithTarget = experiment.featureExtraction.flightSelectedFeatures.map { features =>
        val featureNames = features.keys.toSeq
        if (featureNames.contains(experiment.target)) {
          featureNames
        } else {
          info(s"  - Automatically adding target '${experiment.target}' to flight features")
          featureNames :+ experiment.target
        }
      }.getOrElse {
        flightData.columns.toSeq
      }

      val selectedFlightData = flightData.select(flightFeaturesWithTarget.map(flightData(_)): _*)
      info(s"  - Selected ${flightFeaturesWithTarget.length} flight features")

      val cachedFlightData = selectedFlightData.cache()
      whenDebug {
        val count = cachedFlightData.count()
        debug(s"  - Flight records: ${count}")
      }

      cachedFlightData
    }
  }

  def join(
    flightData: DataFrame,
    weatherData: DataFrame,
    experimentConfig: ExperimentConfig
  )(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    MetricsUtils.withUiLabels(
      groupId = "FeaturePipeline.join",
      desc    = "",
      tags    = "sampling,split,balance"
    ) {
      // Jointure des données
      info("Joining flight and weather data...")

      val weatherOriginDepthHours = experimentConfig.featureExtraction.weatherOriginDepthHours
      val weatherDestinationDepthHours = experimentConfig.featureExtraction.weatherDestinationDepthHours

      // Check if weather join should be skipped (both negative)
      if (weatherOriginDepthHours < 0 && weatherDestinationDepthHours < 0) {
        info("  Both weather depth values are negative - NO weather join, returning flight data only")
        return flightData
      }

      val stepStartTime = System.currentTimeMillis()

      // Add target column to flight features if not already present
      val flightFeaturesWithTarget = experimentConfig.featureExtraction.flightSelectedFeatures.map { features =>
        val featureNames = features.keys.toSeq
        if (featureNames.contains(experimentConfig.target)) {
          featureNames
        } else {
          info(s"  - Automatically adding target '${experimentConfig.target}' to flight features")
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
      info("  - Caching joined data...")
      val cachedJoinedData = joinedData.cache()

      // Force materialization with a single count
      whenDebug{
        val joinedCount = cachedJoinedData.count()
        debug(f"  - Joined records: ${joinedCount}%,d with ${cachedJoinedData.columns.length}%3d columns")
      }

      cachedJoinedData
    }

  }

  def explose(
                data: DataFrame,
                experimentConfig: ExperimentConfig
              )(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    MetricsUtils.withUiLabels(
      groupId = "FeaturePipeline.explose",
      desc    = "flatten weather structs",
      tags    = "weather,flatten,features"
    ) {
      import org.apache.spark.sql.functions._
      import org.apache.spark.sql.types._

      // 1) Colonnes struct météo à aplatir : Wo_h*, Wd_h*
      val weatherStructCols: Seq[StructField] =
        data.schema.fields.collect {
          case f @ StructField(name, st: StructType, _, _)
            if name.matches("(Wo_h\\d+|Wd_h\\d+)") =>
            f
        }

      if (weatherStructCols.isEmpty) {
        info("No weather struct columns found (Wo_h*, Wd_h*). Nothing to explode.")
        return data
      }

      // 2) Features météo à extraire : config → sinon auto-détection sur le premier struct
      val weatherFeatures: Seq[String] =
        experimentConfig.featureExtraction.weatherSelectedFeatures
          .map(_.keys.toSeq)
          .getOrElse {
            val firstStructType = weatherStructCols.head.dataType.asInstanceOf[StructType]
            val dropMeta = Set("hour", "WBAN", "WDATE", "WTIME_HHMM")
            val fields = firstStructType.fieldNames.filterNot(dropMeta.contains).toSeq
            info(s"[Auto-detect] Using ${fields.length} weather fields from struct schema:")
            info(s"  ${fields.mkString(", ")}")
            fields
          }

      info(s"Flattening weather structs:")
      info(s"  - Struct columns: ${weatherStructCols.map(_.name).mkString(", ")}")
      info(s"  - Weather features: ${weatherFeatures.mkString(", ")}")
      info(s"  - Input columns: ${data.columns.length}")

      var result = data
      var totalAddedColumns = 0

      // 3) Aplatissement de chaque struct Wo_h*, Wd_h*
      weatherStructCols.foreach { sf =>
        val structName = sf.name              // ex: "Wd_h1"
        val structType = sf.dataType.asInstanceOf[StructType]

        val isOrigin = structName.startsWith("Wo_")
        val prefix   = if (isOrigin) "origin_weather" else "destination_weather"

        // Récupère "h1", "h2", ...
        val hSuffix: String = "_h(\\d+)".r
          .findFirstMatchIn(structName)
          .map(m => s"h${m.group(1)}")
          .getOrElse("h0")

        // On peut aussi remonter les méta-infos si besoin
        val metaFields = Seq("hour", "WBAN", "WDATE", "WTIME_HHMM")

        metaFields.foreach { fName =>
          if (structType.fieldNames.contains(fName)) {
            val outName = s"${prefix}_${fName}_${hSuffix}"
            result = result.withColumn(outName, col(structName).getField(fName))
            totalAddedColumns += 1
          }
        }

        // Les vraies features météo
        weatherFeatures.foreach { feature =>
          if (structType.fieldNames.contains(feature)) {
            val outName = s"${prefix}_${feature}_${hSuffix}"
            result = result.withColumn(outName, col(structName).getField(feature))
            totalAddedColumns += 1
          } else {
            // optionnel : log en debug si la feature n'existe pas dans le struct
            whenDebug {
              info(s"[explose2] Feature '$feature' not found in struct '$structName'")
            }
          }
        }

        // On supprime le struct une fois aplati
        result = result.drop(structName)
      }

      info(s"  - Total added columns: $totalAddedColumns")
      info(s"  - Output columns: ${result.columns.length}")
      info("  - Caching exploded data...")

      result.cache()
    }
  }
}
