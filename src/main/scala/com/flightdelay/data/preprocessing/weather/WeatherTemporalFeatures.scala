package com.flightdelay.data.preprocessing.weather

import com.flightdelay.config.AppConfiguration
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.flightdelay.utils.DebugUtils._

/**
 * Creates temporal delta features from weather data
 * Captures how fast conditions are changing (momentum)
 */
object WeatherTemporalFeatures {

  /**
   * Add delta features (1-hour change) to weather DataFrame
   *
   * Features created:
   * - Temp_Delta_1hr: Temperature change in 1 hour (°C/hr)
   * - Visibility_Delta_1hr: Visibility change in 1 hour (km/hr)
   * - Humidity_Delta_1hr: Relative humidity change in 1 hour (%/hr)
   * - WindSpeed_Delta_1hr: Wind speed change in 1 hour (km/h per hr)
   *
   * @param df Weather DataFrame with WBAN and temporal columns
   * @return DataFrame with 4 new delta features
   */
  def addDeltaFeatures(df: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    info("- Calling com.flightdelay.data.preprocessing.weather.WeatherTemporalFeatures.addDeltaFeatures()")

    // Window: partition by weather station (WBAN), order by time
    // This ensures we calculate deltas within the same station over time
    val windowSpec = Window
      .partitionBy("WBAN")
      .orderBy("Date", "Time")  // Order by date and time chronologically

    // Define the 4 delta features to create
    val deltaFeatures = Seq(
      ("DryBulbCelsius", "Temp_Delta_1hr"),
      ("Visibility", "Visibility_Delta_1hr"),
      ("RelativeHumidity", "Humidity_Delta_1hr"),
      ("WindSpeed", "WindSpeed_Delta_1hr")
    )

    // Apply delta calculation for each feature
    var result = df
    var createdCount = 0

    deltaFeatures.foreach { case (sourceCol, targetCol) =>
      if (df.columns.contains(sourceCol)) {
        result = result.withColumn(
          targetCol,
          col(sourceCol) - lag(sourceCol, 1).over(windowSpec)
        )
        createdCount += 1
        debug(s"    Created: $targetCol from $sourceCol")
      } else {
        debug(s"    Skipped: $targetCol (source column '$sourceCol' not found)")
      }
    }

    info(s"  - Created $createdCount delta/momentum features")
    result
  }

  /**
   * Preprocess weather data before creating delta features
   * Ensures data is properly sorted and has required columns
   */
  def preprocess(df: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {
    info("- Calling com.flightdelay.data.preprocessing.weather.WeatherTemporalFeatures.preprocess")

    // Add delta features
    val dfWithDeltas = addDeltaFeatures(df)

    dfWithDeltas
  }
}
