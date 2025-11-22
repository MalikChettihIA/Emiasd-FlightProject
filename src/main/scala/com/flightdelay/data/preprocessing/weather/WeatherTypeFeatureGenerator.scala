package com.flightdelay.data.preprocessing.weather

import com.flightdelay.config.AppConfiguration
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.flightdelay.utils.DebugUtils._

object WeatherTypeFeatureGenerator extends Serializable {

  /**
   * Crée les features de base à partir du WeatherType (codes METAR)
   * @param weatherDF DataFrame contenant au minimum la colonne WeatherType
   * @return DataFrame avec les features météo ajoutées
   */
  def createWeatherFeatures(weatherDF: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    info("- Calling com.flightdelay.data.preprocessing.weather.WeatherTypeFeatureGenerator.createWeatherFeatures()")

    weatherDF
      // 1. INTENSITÉ (String Indexing pour ML)
      .withColumn("intensity_heavy",
        when(col("WeatherType").startsWith("+"), 1).otherwise(0).cast(IntegerType))
      .withColumn("intensity_light",
        when(col("WeatherType").startsWith("-"), 1).otherwise(0).cast(IntegerType))
      .withColumn("weather_intensity",
        when(col("WeatherType").startsWith("+"), "heavy")
          .when(col("WeatherType").startsWith("-"), "light")
          .otherwise("moderate"))
      // Version ordinale de l'intensité des précipitations (-1, 0, +1)
      // Plus pratique pour les modèles ML que deux colonnes binaires séparées
      .withColumn("feature_precipitation_intensity",
        when(col("WeatherType").startsWith("+"), lit(1))   // Heavy precipitation (+)
          .when(col("WeatherType").startsWith("-"), lit(-1))  // Light precipitation (-)
          .otherwise(lit(0))  // Moderate precipitation (no prefix)
          .cast(IntegerType))

      // 2. ORAGE (TS)
      .withColumn("has_thunderstorm",
        when(col("WeatherType").contains("TS"), 1).otherwise(0).cast(IntegerType))

      // 3. GIVRAGE CRITIQUE - Précipitations givrantes
      .withColumn("has_freezing_precip",
        when(col("WeatherType").contains("FZRA") ||
          col("WeatherType").contains("FZDZ") ||
          col("WeatherType").contains("FZFG") ||
          col("WeatherType").contains("FZBR"), 1)
          .otherwise(0).cast(IntegerType))

      // 4. GIVRAGE GÉNÉRAL (FZ)
      .withColumn("has_freezing",
        when(col("WeatherType").contains("FZ"), 1).otherwise(0).cast(IntegerType))

      // 5. PRÉCIPITATIONS
      .withColumn("has_precipitation",
        when(col("WeatherType").contains("RA") ||  // Rain
          col("WeatherType").contains("DZ") ||  // Drizzle
          col("WeatherType").contains("SN") ||  // Snow
          col("WeatherType").contains("GR") ||  // Hail
          col("WeatherType").contains("GS") ||  // Small Hail
          col("WeatherType").contains("PL") ||  // Ice Pellets
          col("WeatherType").contains("SG") ||  // Snow Grains
          col("WeatherType").contains("IC") ||  // Ice Crystals
          col("WeatherType").contains("UP"), 1) // Unknown Precip
          .otherwise(0).cast(IntegerType))

      // 6. OBSCURATION (Visibilité réduite)
      .withColumn("has_obscuration",
        when(col("WeatherType").contains("BR") ||  // Mist
          col("WeatherType").contains("FG") ||  // Fog
          col("WeatherType").contains("FU") ||  // Smoke
          col("WeatherType").contains("HZ") ||  // Haze
          col("WeatherType").contains("DU") ||  // Dust
          col("WeatherType").contains("SA") ||  // Sand
          col("WeatherType").contains("VA"), 1) // Volcanic Ash
          .otherwise(0).cast(IntegerType))

      // 7. HUMIDITÉ VISIBLE (pour Icing Risk)
      .withColumn("has_visible_moisture",
        when(col("WeatherType").contains("RA") ||
          col("WeatherType").contains("DZ") ||
          col("WeatherType").contains("FG") ||
          col("WeatherType").contains("BR") ||
          col("WeatherType").contains("SH"), 1)
          .otherwise(0).cast(IntegerType))

      // 8. CONDITIONS DANGEREUSES
      .withColumn("has_hazardous",
        when(col("WeatherType").contains("DS") ||  // Dust Storm
          col("WeatherType").contains("SS") ||  // Sand Storm
          col("WeatherType").contains("FC") ||  // Funnel Cloud
          col("WeatherType").contains("SQ") ||  // Squall
          col("WeatherType").contains("PO") ||  // Dust Whirls
          col("WeatherType").contains("TS") ||  // Thunderstorm
          col("WeatherType").contains("FZ"), 1) // Freezing
          .otherwise(0).cast(IntegerType))

      // 9. TYPES SPÉCIFIQUES DE PRÉCIPITATIONS
      .withColumn("has_rain",
        when(col("WeatherType").contains("RA"), 1).otherwise(0).cast(IntegerType))
      .withColumn("has_snow",
        when(col("WeatherType").contains("SN"), 1).otherwise(0).cast(IntegerType))
      .withColumn("has_drizzle",
        when(col("WeatherType").contains("DZ"), 1).otherwise(0).cast(IntegerType))
      .withColumn("has_fog_mist",
        when(col("WeatherType").contains("FG") || col("WeatherType").contains("BR"), 1)
          .otherwise(0).cast(IntegerType))
      .withColumn("has_hail",
        when(col("WeatherType").contains("GR") || col("WeatherType").contains("GS"), 1)
          .otherwise(0).cast(IntegerType))

      // 10. EXTRACTION DES CODES (pour analyse)
      .withColumn("extracted_codes",
        regexp_replace(col("WeatherType"), "^[+-]", ""))

      // 11. NIVEAU DE DANGER MÉTÉO (0-3)
      .withColumn("weather_hazard_level",
        when(col("has_freezing_precip") === 1, 3)
          .when(col("has_thunderstorm") === 1, 3)
          .when(col("has_hazardous") === 1, 2)
          .when(col("intensity_heavy") === 1 && col("has_precipitation") === 1, 2)
          .when(col("has_precipitation") === 1 || col("has_obscuration") === 1, 1)
          .otherwise(0).cast(IntegerType))
  }

  /**
   * Ajoute les features de risque de givrage (Icing Risk)
   * Nécessite les colonnes: WeatherType, DryBulbCelsius, RelativeHumidity
   * @param weatherDF DataFrame avec données météo
   * @return DataFrame avec Icing_Risk_Flag et Icing_Risk_Level
   */
  def addIcingRiskFeatures(weatherDF: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    info("- Calling com.flightdelay.data.preprocessing.weather.WeatherTypeFeatureGenerator.addIcingRiskFeatures()")

    // D'abord créer les features de base
    val withFeatures = createWeatherFeatures(weatherDF)

    withFeatures
      // Calculer le Temp_Dewpoint_Spread pour le risque de givrage
      .withColumn("_temp_dewpoint_spread",
        col("DryBulbCelsius") - col("DewPointCelsius")
      )

      // ICING RISK FLAG (binaire 0/1) - selon spécification
      // Auto-Trigger: Freezing_Precip actif
      // Conditional Trigger: Temp entre -15 et 2°C ET (spread <= 3 OU Rain OU Drizzle OU Fog_Mist OU Snow)
      .withColumn("feature_icing_risk_flag",
        when(col("has_freezing_precip") === 1, 1)
          .when(col("DryBulbCelsius").isNotNull &&
            col("DryBulbCelsius").between(-15.0, 2.0) &&
            (
              (col("_temp_dewpoint_spread").isNotNull && col("_temp_dewpoint_spread") <= 3.0) ||
              col("has_rain") === 1 ||
              col("has_drizzle") === 1 ||
              col("has_fog_mist") === 1 ||
              col("has_snow") === 1
            ), 1)
          .otherwise(0).cast(IntegerType))

      // ICING RISK LEVEL (0-3)
      .withColumn("feature_icing_risk_level",
        when(col("has_freezing_precip") === 1, 3)
          .when(col("DryBulbCelsius").between(-10.0, 0.0) &&
            (col("has_rain") === 1 || col("has_drizzle") === 1 || col("has_fog_mist") === 1 || col("has_snow") === 1) &&
            col("_temp_dewpoint_spread").isNotNull && col("_temp_dewpoint_spread") <= 2.0, 2)
          .when(col("DryBulbCelsius").between(-15.0, 2.0) &&
            (col("_temp_dewpoint_spread") <= 3.0 || col("has_rain") === 1 || col("has_drizzle") === 1 || col("has_fog_mist") === 1 || col("has_snow") === 1), 1)
          .otherwise(0).cast(IntegerType))

      // Nettoyer la colonne temporaire
      .drop("_temp_dewpoint_spread")
  }

  /**
   * Alias pour addIcingRiskFeatures (pour compatibilité avec votre code)
   */
  def createFeatures(weatherDF: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {
    addIcingRiskFeatures(weatherDF)
  }
}
