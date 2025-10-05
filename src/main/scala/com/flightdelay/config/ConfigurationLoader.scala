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
    val environment = if (args.length > 0) args(0) else "local2"
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

    val source = Source.fromInputStream(inputStream)
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

    // Parse data config
    val dataData = commonData("data").asInstanceOf[java.util.Map[String, Any]].asScala.toMap
    val dataConfig = parseDataConfig(dataData)

    // Parse output config
    val outputData = commonData("output").asInstanceOf[java.util.Map[String, Any]].asScala.toMap
    val basePath = outputData("basePath").toString

    val outputConfig = OutputConfig(
      basePath = basePath,
      data = FileConfig(path = s"$basePath/data"),
      model = FileConfig(path = s"$basePath/model")
    )

    // Parse mlflow config (optional)
    val mlflowConfig = commonData.get("mlflow").map { mlflowData =>
      val mlflowMap = mlflowData.asInstanceOf[java.util.Map[String, Any]].asScala.toMap
      MLFlowConfig(
        enabled = mlflowMap.get("enabled").map(_.toString.toBoolean).getOrElse(false),
        trackingUri = mlflowMap.get("trackingUri").map(_.toString).getOrElse("http://localhost:5555")
      )
    }.getOrElse(MLFlowConfig())

    CommonConfig(
      seed = seed,
      data = dataConfig,
      output = outputConfig,
      mlflow = mlflowConfig
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
    val pcaVarianceThreshold = data.get("pcaVarianceThreshold")
      .map(_.toString.toDouble)
      .getOrElse(0.95)

    // Parse selectedFeatures (optional, for feature_selection type)
    val selectedFeatures = data.get("selectedFeatures").map { featureList =>
      featureList.asInstanceOf[java.util.List[_]].asScala.map(_.toString).toSeq
    }

    FeatureExtractionConfig(
      featureType = featureType,
      pcaVarianceThreshold = pcaVarianceThreshold,
      selectedFeatures = selectedFeatures
    )
  }

  /**
   * Parse experiment model configuration
   */
  private def parseExperimentModelConfig(data: Map[String, Any]): ExperimentModelConfig = {
    val modelType = data("modelType").toString

    ExperimentModelConfig(
      modelType = modelType
    )
  }

  /**
   * Parse train configuration
   */
  private def parseTrainConfig(trainData: Map[String, Any]): TrainConfig = {
    val trainRatio = trainData("trainRatio").toString.toDouble

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

    // Parse hyperparameters
    val hyperparametersData = trainData("hyperparameters").asInstanceOf[java.util.Map[String, Any]].asScala.toMap
    val hyperparametersConfig = parseHyperparametersConfig(hyperparametersData)

    TrainConfig(
      trainRatio = trainRatio,
      crossValidation = crossValidationConfig,
      gridSearch = gridSearchConfig,
      hyperparameters = hyperparametersConfig
    )
  }

  /**
   * Parse hyperparameters configuration
   */
  private def parseHyperparametersConfig(data: Map[String, Any]): HyperparametersConfig = {
    // numTrees can be an array or a single value
    val numTrees = data("numTrees") match {
      case list: java.util.List[_] => list.asScala.map(_.toString.toInt).toSeq
      case value => Seq(value.toString.toInt)
    }

    // maxDepth can be an array or a single value
    val maxDepth = data("maxDepth") match {
      case list: java.util.List[_] => list.asScala.map(_.toString.toInt).toSeq
      case value => Seq(value.toString.toInt)
    }

    val maxBins = data("maxBins").toString.toInt
    val minInstancesPerNode = data("minInstancesPerNode").toString.toInt
    val subsamplingRate = data("subsamplingRate").toString.toDouble
    val featureSubsetStrategy = data("featureSubsetStrategy").toString
    val impurity = data("impurity").toString

    HyperparametersConfig(
      numTrees = numTrees,
      maxDepth = maxDepth,
      maxBins = maxBins,
      minInstancesPerNode = minInstancesPerNode,
      subsamplingRate = subsamplingRate,
      featureSubsetStrategy = featureSubsetStrategy,
      impurity = impurity
    )
  }
}
