package com.flightdelay.data.loaders

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import scala.util.Try

/**
 * Base trait for all data loaders in the flight delay prediction system
 * @tparam T The type of domain object this loader produces
 */
trait DataLoader[T] {
  
  /**
   * Load raw data from the specified path and return as DataFrame
   * @param path The path to the data source
   * @param spark Implicit SparkSession
   * @return DataFrame containing the raw data
   */
  def loadRaw(path: String)(implicit spark: SparkSession): Try[DataFrame]
  
  /**
   * Load and transform data into domain objects
   * @param path The path to the data source
   * @param spark Implicit SparkSession
   * @return DataFrame with properly typed domain objects
   */
  def load(path: String)(implicit spark: SparkSession): Try[DataFrame]
  
  /**
   * Load data with additional filtering options
   * @param path The path to the data source
   * @param filters Map of column filters to apply
   * @param spark Implicit SparkSession
   * @return Filtered DataFrame
   */
  def loadWithFilters(path: String, filters: Map[String, Any])
                     (implicit spark: SparkSession): Try[DataFrame]
  
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
  
  /**
   * Clean and preprocess the raw data
   * @param rawDf Raw DataFrame from data source
   * @return Cleaned DataFrame
   */
  def cleanData(rawDf: DataFrame): DataFrame
  
  /**
   * Get basic statistics about the loaded data
   * @param df The DataFrame to analyze
   * @return Map containing statistics
   */
  def getDataStatistics(df: DataFrame): Map[String, Any]
}