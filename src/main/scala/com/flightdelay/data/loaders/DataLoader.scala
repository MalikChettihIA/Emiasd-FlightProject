package com.flightdelay.data.loaders

import com.flightdelay.config.AppConfiguration
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

import scala.util.Try

/**
 * Base trait for all data loaders in the flight delay prediction system
 * @tparam T The type of domain object this loader produces
 */
trait DataLoader[T] {

  /**
   * Load and transform data into domain objects
   * @param configuration The configuration of the application
   * @param spark Implicit SparkSession
   * @return DataFrame with properly typed domain objects
   */
  def loadFromConfiguration(configuration: AppConfiguration, validate: Boolean)(implicit spark: SparkSession): DataFrame

  /**
   * Load and transform data into domain objects
   * @param configuration The configuration of the application
   * @param spark Implicit SparkSession
   * @return DataFrame with properly typed domain objects
   */
  def loadFromFilePath(filePath: String, validate: Boolean)(implicit spark: SparkSession): DataFrame
}