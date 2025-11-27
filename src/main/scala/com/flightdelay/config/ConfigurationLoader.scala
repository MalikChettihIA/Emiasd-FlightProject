package com.flightdelay.config

import org.yaml.snakeyaml.Yaml

import scala.io.Source
import scala.jdk.CollectionConverters.{collectionAsScalaIterableConverter, mapAsScalaMapConverter}
import scala.util.Try

object ConfigurationLoader {

  /**
   * Charge la configuration depuis les arguments de ligne de commande
   */
  def loadConfiguration(args: Array[String]): AppConfiguration = {
    val environment = if (args.length > 0) args(0) else "local"
    loadEnvironment(environment)
  }

  /**
   * Charge la configuration selon l'environnement spécifié
   */
  private def loadEnvironment(environment: String): AppConfiguration = {
    val configFile = environment.toLowerCase + "-config.yml"
    loadConfigFromFile(configFile)
  }

  /**
   * Charge la configuration depuis un fichier YAML
   */
  private def loadConfigFromFile(filename: String): AppConfiguration = {
    val inputStream = getClass.getClassLoader.getResourceAsStream(filename)
    if (inputStream == null) {
      throw new RuntimeException(s"Fichier de configuration non trouvé: $filename")
    }

    val source = Source.fromInputStream(inputStream, "UTF-8")
    val yamlContent = source.mkString
    source.close()

    // Parser le YAML
    val yaml = new Yaml()
    val data = yaml.load(yamlContent).asInstanceOf[java.util.Map[String, Any]]

    // Convertir en case classes Scala
    parseConfiguration(data.asScala.toMap)
  }

  /**
   * Parse la map YAML en case classes Scala
   */
  private def parseConfiguration(data: Map[String, Any]): AppConfiguration = {
    val environment = data("environment").toString

    // --- Parse Common Configuration ---
    val commonData = data("common").asInstanceOf[java.util.Map[String, Any]].asScala.toMap
    val commonConfig = parseCommonConfig(commonData)

    // --- Parse Experiments ---
    val experimentsData = data("experiments").asInstanceOf[java.util.List[Any]].asScala.toSeq
    val experiments = experimentsData.map { exp =>
      val expMap = exp.asInstanceOf[java.util.Map[String, Any]].asScala.toMap
      parseExperimentConfig(expMap)
    }

    AppConfiguration(
      environment = environment,
      common = commonConfig,
      experiments = experiments
    )
  }

  /**
   * Parse common configuration
   */
  private def parseCommonConfig(commonData: Map[String, Any]): CommonConfig = {
    val seed = commonData("seed").toString.toLong

    // Parse logging configuration (optional)
    val log = commonData.get("log")
      .map(_.toString.toBoolean)
      .getOrElse(true)

    val logLevel = commonData.get("logLevel")
      .map(_.toString)
      .getOrElse("info")

    val loadDataFromCSV = commonData.get("loadDataFromCSV")
      .map(_.toString.toBoolean)
      .getOrElse(true)

    val storeIntoParquet = commonData.get("storeIntoParquet")
      .map(_.toString.toBoolean)
      .getOrElse(true)

    // Parse data config
    val dataData = commonData("data").asInstanceOf[java.util.Map[String, Any]].asScala.toMap
    val dataConfig = parseDataConfig(dataData)

    // Parse output config
    val outputData = commonData("output").asInstanceOf[java.util.Map[String, Any]].asScala.toMap
    val basePath = outputData("basePath").toString

    // Parse optional localPath (for HDFS to local copy, e.g., CephFS)
    val localPath = outputData.get("localPath").map(_.toString)

    val outputConfig = OutputConfig(
      basePath = basePath,
      data = FileConfig(path = s"$basePath/data"),
      model = FileConfig(path = s"$basePath/model"),
      localPath = localPath
    )

    // Parse mlflow config (optional)
    val mlflowConfig = commonData.get("mlflow").map { mlflowData =>
      val mlflowMap = mlflowData.asInstanceOf[java.util.Map[String, Any]].asScala.toMap
      MLFlowConfig(
        enabled = mlflowMap.get("enabled").map(_.toString.toBoolean).getOrElse(false),
        trackingUri = mlflowMap.get("trackingUri").map(_.toString).getOrElse("http://localhost:5555")
      )
    }.getOrElse(MLFlowConfig())

    // Parse scriptsPath (optional, defaults to /scripts)
    val scriptsPath = commonData.get("scriptsPath")
      .map(_.toString)
      .getOrElse("/scripts")

    CommonConfig(
      seed = seed,
      log = log,
      logLevel = logLevel,
      loadDataFromCSV = loadDataFromCSV,
      storeIntoParquet = storeIntoParquet,
      data = dataConfig,
      output = outputConfig,
      mlflow = mlflowConfig,
      scriptsPath = scriptsPath
    )
  }

  /**
   * Parse data configuration
   */
  private def parseDataConfig(dataData: Map[String, Any]): DataConfig = {
    val basePath = dataData("basePath").toString
    val flightData = dataData("flight").asInstanceOf[java.util.Map[String, Any]].asScala.toMap
    val weatherData = dataData("weather").asInstanceOf[java.util.Map[String, Any]].asScala.toMap
    val airportData = dataData("airportMapping").asInstanceOf[java.util.Map[String, Any]].asScala.toMap

    DataConfig(
      basePath = basePath,
      flight = FileConfig(path = flightData("path").toString),
      weather = FileConfig(path = weatherData("path").toString),
      airportMapping = FileConfig(path = airportData("path").toString)
    )
  }

  /**
   * Parse experiment configuration
   */
  private def parseExperimentConfig(expData: Map[String, Any]): ExperimentConfig = {
    val name = expData("name").toString
    val description = expData("description").toString
    val enabled = expData("enabled").toString.toBoolean
    val target = expData("target").toString


    // Parse feature extraction
    val featureExtractionData = expData("featureExtraction").asInstanceOf[java.util.Map[String, Any]].asScala.toMap
    val featureExtractionConfig = parseFeatureExtractionConfig(featureExtractionData)

    // Parse model
    val modelData = expData("model").asInstanceOf[java.util.Map[String, Any]].asScala.toMap
    val modelConfig = parseExperimentModelConfig(modelData)

    // Parse train configuration
    val trainData = expData("train").asInstanceOf[java.util.Map[String, Any]].asScala.toMap
    val trainConfig = parseTrainConfig(trainData)

    ExperimentConfig(
      name = name,
      description = description,
      enabled = enabled,
      target = target,
      featureExtraction = featureExtractionConfig,
      model = modelConfig,
      train = trainConfig
    )
  }

  /**
   * Parse feature extraction configuration
   */
  private def parseFeatureExtractionConfig(data: Map[String, Any]): FeatureExtractionConfig = {
    val featureType = data("type").toString
    val dxCol = data("dxCol").toString

    val pcaVarianceThreshold = data.get("pcaVarianceThreshold")
      .map(_.toString.toDouble)
      .getOrElse(0.95)

    val delayThresholdMin = data.get("delayThresholdMin")
      .map(_.toString.toInt)
      .getOrElse(60)

    // Parse storeJoinData (optional, defaults to false)
    val storeJoinData = data.get("storeJoinData")
      .map(_.toString.toBoolean)
      .getOrElse(false)

    // Parse storeExplodeJoinData (optional, defaults to false)
    val storeExplodeJoinData = data.get("storeExplodeJoinData")
      .map(_.toString.toBoolean)
      .getOrElse(false)

    // Parse weatherDepartureDepthHours (optional, defaults to 0)
    val weatherOriginDepthHours = data.get("weatherOriginDepthHours")
      .map(_.toString.toInt)
      .getOrElse(0)

    // Parse weatherArrivalDepthHours (optional, defaults to 0)
    val weatherDestinationDepthHours = data.get("weatherDestinationDepthHours")
      .map(_.toString.toInt)
      .getOrElse(0)

    // Parse maxCategoricalCardinality (optional, defaults to 50)
    val maxCategoricalCardinality = data.get("maxCategoricalCardinality")
      .map(_.toString.toInt)
      .getOrElse(50)

    // Parse handleInvalid (optional, defaults to "keep")
    val handleInvalid = data.get("handleInvalid")
      .map(_.toString)
      .getOrElse("keep")

    // Parse flightSelectedFeatures (optional, for feature_selection type)
    // New format: Map[String, FeatureTransformationConfig]
    val flightSelectedFeatures = data.get("flightSelectedFeatures").map { featuresMap =>
      featuresMap.asInstanceOf[java.util.Map[String, Any]].asScala.toMap.map { case (featureName, config) =>
        val configMap = config.asInstanceOf[java.util.Map[String, Any]].asScala.toMap
        val transformation = configMap("transformation").toString
        (featureName, FeatureTransformationConfig(transformation))
      }
    }

    // Parse weatherSelectedFeatures (optional, for feature_selection type)
    // New format: Map[String, FeatureTransformationConfig]
    val weatherSelectedFeatures = data.get("weatherSelectedFeatures").map { featuresMap =>
      featuresMap.asInstanceOf[java.util.Map[String, Any]].asScala.toMap.map { case (featureName, config) =>
        val configMap = config.asInstanceOf[java.util.Map[String, Any]].asScala.toMap
        val transformation = configMap("transformation").toString
        (featureName, FeatureTransformationConfig(transformation))
      }
    }

    // Parse aggregatedSelectedFeatures (optional, for accumulation features)
    // Format: Map[String, AggregatedFeatureConfig]
    val aggregatedSelectedFeatures = data.get("aggregatedSelectedFeatures").map { featuresMap =>
      featuresMap.asInstanceOf[java.util.Map[String, Any]].asScala.toMap.map { case (featureName, config) =>
        val configMap = config.asInstanceOf[java.util.Map[String, Any]].asScala.toMap
        val aggregation = configMap("aggregation").toString
        val transformation = configMap.get("transformation").map(_.toString).getOrElse("None")
        (featureName, AggregatedFeatureConfig(aggregation, transformation))
      }
    }

    FeatureExtractionConfig(
      featureType = featureType,
      dxCol = dxCol,
      delayThresholdMin = delayThresholdMin,
      pcaVarianceThreshold = pcaVarianceThreshold,
      storeJoinData = storeJoinData,
      storeExplodeJoinData = storeExplodeJoinData,
      weatherOriginDepthHours = weatherOriginDepthHours,
      weatherDestinationDepthHours = weatherDestinationDepthHours,
      maxCategoricalCardinality = maxCategoricalCardinality,
      handleInvalid = handleInvalid,
      flightSelectedFeatures = flightSelectedFeatures,
      weatherSelectedFeatures = weatherSelectedFeatures,
      aggregatedSelectedFeatures = aggregatedSelectedFeatures
    )
  }

  /**
   * Parse experiment model configuration
   */
  private def parseExperimentModelConfig(data: Map[String, Any]): ExperimentModelConfig = {
    val modelType = data("modelType").toString

    // Parse hyperparameters (now under model)
    val hyperparametersData = data("hyperparameters").asInstanceOf[java.util.Map[String, Any]].asScala.toMap
    val hyperparametersConfig = parseHyperparametersConfig(hyperparametersData)

    ExperimentModelConfig(
      modelType = modelType,
      hyperparameters = hyperparametersConfig
    )
  }

  /**
   * Parse train configuration
   */
  private def parseTrainConfig(trainData: Map[String, Any]): TrainConfig = {
    val trainRatio = trainData("trainRatio").toString.toDouble

    // Parse fast flag (default: false)
    val fast = trainData.getOrElse("fast", false).toString.toBoolean

    // Parse cross-validation
    val cvData = trainData("crossValidation").asInstanceOf[java.util.Map[String, Any]].asScala.toMap
    val crossValidationConfig = CrossValidationConfig(
      numFolds = cvData("numFolds").toString.toInt
    )

    // Parse grid search
    val gridSearchData = trainData("gridSearch").asInstanceOf[java.util.Map[String, Any]].asScala.toMap
    val gridSearchConfig = GridSearchConfig(
      enabled = gridSearchData("enabled").toString.toBoolean,
      evaluationMetric = gridSearchData("evaluationMetric").toString
    )

    TrainConfig(
      trainRatio = trainRatio,
      fast = fast,
      crossValidation = crossValidationConfig,
      gridSearch = gridSearchConfig
    )
  }

  /**
   * Parse hyperparameters configuration
   * All parameters support arrays for grid search
   */
  private def parseHyperparametersConfig(data: Map[String, Any]): HyperparametersConfig = {
    // Tree-based model parameters (optional)
    val numTrees = data.get("numTrees").map {
      case list: java.util.List[_] => list.asScala.map(_.toString.toInt).toSeq
      case value => Seq(value.toString.toInt)
    }

    val maxDepth = data.get("maxDepth").map {
      case list: java.util.List[_] => list.asScala.map(_.toString.toInt).toSeq
      case value => Seq(value.toString.toInt)
    }

    val maxBins = data.get("maxBins").map {
      case list: java.util.List[_] => list.asScala.map(_.toString.toInt).toSeq
      case value => Seq(value.toString.toInt)
    }

    val minInstancesPerNode = data.get("minInstancesPerNode").map {
      case list: java.util.List[_] => list.asScala.map(_.toString.toInt).toSeq
      case value => Seq(value.toString.toInt)
    }

    val subsamplingRate = data.get("subsamplingRate").map {
      case list: java.util.List[_] => list.asScala.map(_.toString.toDouble).toSeq
      case value => Seq(value.toString.toDouble)
    }

    val featureSubsetStrategy = data.get("featureSubsetStrategy").map {
      case list: java.util.List[_] => list.asScala.map(_.toString).toSeq
      case value => Seq(value.toString)
    }

    val impurity = data.get("impurity").map(_.toString)

    // GBT specific parameters (optional)
    val stepSize = data.get("stepSize").map {
      case list: java.util.List[_] => list.asScala.map(_.toString.toDouble).toSeq
      case value => Seq(value.toString.toDouble)
    }

    // Logistic Regression parameters (optional)
    val maxIter = data.get("maxIter").map {
      case list: java.util.List[_] => list.asScala.map(_.toString.toInt).toSeq
      case value => Seq(value.toString.toInt)
    }

    val regParam = data.get("regParam").map {
      case list: java.util.List[_] => list.asScala.map(_.toString.toDouble).toSeq
      case value => Seq(value.toString.toDouble)
    }

    val elasticNetParam = data.get("elasticNetParam").map {
      case list: java.util.List[_] => list.asScala.map(_.toString.toDouble).toSeq
      case value => Seq(value.toString.toDouble)
    }

    // XGBoost specific parameters (optional)
    val alpha = data.get("alpha").map {
      case list: java.util.List[_] => list.asScala.map(_.toString.toDouble).toSeq
      case value => Seq(value.toString.toDouble)
    }

    val lambda = data.get("lambda").map {
      case list: java.util.List[_] => list.asScala.map(_.toString.toDouble).toSeq
      case value => Seq(value.toString.toDouble)
    }

    val gamma = data.get("gamma").map {
      case list: java.util.List[_] => list.asScala.map(_.toString.toDouble).toSeq
      case value => Seq(value.toString.toDouble)
    }

    val colsampleBytree = data.get("colsampleBytree").map {
      case list: java.util.List[_] => list.asScala.map(_.toString.toDouble).toSeq
      case value => Seq(value.toString.toDouble)
    }

    HyperparametersConfig(
      numTrees = numTrees,
      maxDepth = maxDepth,
      maxBins = maxBins,
      minInstancesPerNode = minInstancesPerNode,
      subsamplingRate = subsamplingRate,
      featureSubsetStrategy = featureSubsetStrategy,
      impurity = impurity,
      stepSize = stepSize,
      maxIter = maxIter,
      regParam = regParam,
      elasticNetParam = elasticNetParam,
      alpha = alpha,
      lambda = lambda,
      gamma = gamma,
      colsampleBytree = colsampleBytree
    )
  }
}
