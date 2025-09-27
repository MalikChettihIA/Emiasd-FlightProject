package com.flightdelay.app

import com.flightdelay.config.AppConfig
import com.flightdelay.data.loaders.{FlightDataLoader, WeatherDataLoader}
import com.flightdelay.data.joiners.FlightWeatherJoiner
import com.flightdelay.features.FeatureExtractor
import com.flightdelay.ml.pipeline.MLPipeline
import com.flightdelay.utils.SparkSession

import org.apache.spark.sql.SparkSession

object FlightDelayPredictionApp {
  
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.getOrCreateSession()
    
    try {
      println("Starting Flight Delay Prediction Pipeline...")
      
      // 1. Data Loading
      val flightData = FlightDataLoader.load(AppConfig.Data.flightDataPath)
      val weatherData = WeatherDataLoader.load(AppConfig.Data.weatherDataPath)
      
      // 2. Data Joining
      val joinedData = FlightWeatherJoiner.join(flightData, weatherData)
      
      // 3. Feature Extraction
      val featuredData = FeatureExtractor.extract(joinedData)
      
      // 4. ML Pipeline
      val pipeline = new MLPipeline()
      val model = pipeline.train(featuredData)
      
      // 5. Model Evaluation
      val results = pipeline.evaluate(model, featuredData)
      
      println("Pipeline completed successfully!")
      results.show()
      
    } catch {
      case ex: Exception =>
        println(s"Error in pipeline: ${ex.getMessage}")
        ex.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}