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
  def load(configuration: AppConfiguration, validate: Boolean)(implicit spark: SparkSession): Try[DataFrame]

  /**
   * Load raw data from the specified path and return as DataFrame
   * @param path The path to the data source
   * @param spark Implicit SparkSession
   * @return DataFrame containing the raw data
   */
  def loadRaw(path: String)(implicit spark: SparkSession): Try[DataFrame]
  
  /**
   * Validate the loaded data structure
   * @param df The DataFrame to validate
   * @return True if data structure is valid
   */
  def validateSchema(df: DataFrame): Boolean
  
  /**
   * Get the expected schema for this data type
   * @return StructType representing the expected schema
   */
  def expectedSchema: StructType


}