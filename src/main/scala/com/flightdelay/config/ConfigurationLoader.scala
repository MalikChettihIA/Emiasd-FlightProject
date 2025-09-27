package com.flightdelay.config

import org.yaml.snakeyaml.Yaml

import scala.io.Source
import scala.jdk.CollectionConverters.mapAsScalaMapConverter
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

    // Parse Data config
    val dataData = data("data").asInstanceOf[java.util.Map[String, Any]].asScala.toMap
    val flightData = dataData("flight").asInstanceOf[java.util.Map[String, Any]].asScala.toMap
    val weatherData = dataData("weather").asInstanceOf[java.util.Map[String, Any]].asScala.toMap
    val airportData = dataData("airportMapping").asInstanceOf[java.util.Map[String, Any]].asScala.toMap

    val dataConfig = DataConfig(
      basePath = dataData("basePath").toString,
      flight = DataFileConfig(path = flightData("path").toString),
      weather = DataFileConfig(path = weatherData("path").toString),
      airportMapping = DataFileConfig(path = airportData("path").toString)
    )

    AppConfiguration(
      environment = data("environment").toString,
      data = dataConfig
    )
  }
}
