package com.flightdelay.data.preprocessing.weather

import com.flightdelay.config.AppConfiguration
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.flightdelay.utils.DebugUtils._

object WeatherInteractionFeatures {

  // Map Code → Weather_Type
  private val weatherTypeMap: Map[Int, String] = Map(
    21 -> "Rain", 60 -> "Rain", 61 -> "Rain", 62 -> "Rain", 63 -> "Rain", 64 -> "Rain", 65 -> "Rain",
    24 -> "Freezing_Precip", 56 -> "Freezing_Precip", 57 -> "Freezing_Precip", 66 -> "Freezing_Precip", 67 -> "Freezing_Precip",
    20 -> "Drizzle", 50 -> "Drizzle", 51 -> "Drizzle", 52 -> "Drizzle", 53 -> "Drizzle", 54 -> "Drizzle", 55 -> "Drizzle",
    22 -> "Snow", 70 -> "Snow", 71 -> "Snow", 72 -> "Snow", 73 -> "Snow", 74 -> "Snow", 75 -> "Snow",
    23 -> "Rain_Snow_Mix", 68 -> "Rain_Snow_Mix", 69 -> "Rain_Snow_Mix",
    25 -> "Shower", 26 -> "Shower", 80 -> "Shower", 81 -> "Shower", 82 -> "Shower",
    83 -> "Shower", 84 -> "Shower", 85 -> "Shower", 86 -> "Shower",
    17 -> "Thunderstorm", 29 -> "Thunderstorm", 95 -> "Thunderstorm",
    96 -> "Thunderstorm", 97 -> "Thunderstorm", 99 -> "Thunderstorm",
    10 -> "Fog_Mist", 28 -> "Fog_Mist", 40 -> "Fog_Mist", 41 -> "Fog_Mist",
    42 -> "Fog_Mist", 43 -> "Fog_Mist", 44 -> "Fog_Mist", 45 -> "Fog_Mist",
    5  -> "Haze",
    18 -> "Squall",
    30 -> "Dust_Sand", 31 -> "Dust_Sand", 32 -> "Dust_Sand", 33 -> "Dust_Sand",
    34 -> "Dust_Sand", 35 -> "Dust_Sand",
    36 -> "Blowing_Snow", 37 -> "Blowing_Snow", 38 -> "Blowing_Snow", 39 -> "Blowing_Snow"
  )

  // Map Code → Weather_Intensity
  private val weatherIntensityMap: Map[Int, String] = Map(
    50 -> "Light", 51 -> "Light", 60 -> "Light", 61 -> "Light", 70 -> "Light", 71 -> "Light",
    80 -> "Light", 83 -> "Light", 85 -> "Light",
    52 -> "Moderate", 53 -> "Moderate", 62 -> "Moderate", 63 -> "Moderate",
    72 -> "Moderate", 73 -> "Moderate", 81 -> "Moderate", 84 -> "Moderate", 86 -> "Moderate",
    54 -> "Heavy", 55 -> "Heavy", 64 -> "Heavy", 65 -> "Heavy",
    74 -> "Heavy", 75 -> "Heavy", 82 -> "Heavy", 87 -> "Heavy", 88 -> "Heavy",
    97 -> "Heavy", 99 -> "Heavy"
  )

  // Spark Column expressions pour les maps
  private val weatherTypeMapCol = map(weatherTypeMap.flatMap { case (k, v) => Seq(lit(k), lit(v)) }.toSeq: _*)
  private val weatherIntensityMapCol = map(weatherIntensityMap.flatMap { case (k, v) => Seq(lit(k), lit(v)) }.toSeq: _*)

  /**
   * Applique toutes les features d'interaction
   * OPTIMISÉ : Utilise uniquement des expressions Spark natives (pas d'UDF)
   */
  def createInteractionFeatures(df: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    info("- Calling com.flightdelay.data.preprocessing.weather.WeatherInteractionFeatures.createInteractionFeatures()")

    df
      // 1. Calculer l'indice de sévérité météorologique combiné
      // Combinaison pondérée + pénalités pour conditions critiques
      .withColumn("_temp_base_score",
        (col("feature_cloud_risk_score") * 0.4) + (col("feature_visibility_risk_score") * 0.6)
      )

      .withColumn("_temp_ceiling_penalty",
        when(col("feature_ceiling") < 500, lit(2.0))
          .when(col("feature_ceiling") < 1000, lit(1.0))
          .otherwise(lit(0.0))
      )

      .withColumn("_temp_visibility_penalty",
        when(col("feature_visibility_miles") < 0.5, lit(2.0))
          .when(col("feature_visibility_miles") < 1.0, lit(1.0))
          .otherwise(lit(0.0))
      )

      .withColumn("feature_weather_severity_index",
        least(
          col("_temp_base_score") + col("_temp_ceiling_penalty") + col("_temp_visibility_penalty"),
          lit(10.0)
        )
      )

      // 2. Déterminer si les conditions sont VFR (Visual Flight Rules)
      .withColumn("feature_is_vfr_conditions",
        ((col("feature_visibility_miles") >= 5.0) &&
          (col("feature_ceiling") >= 3000)).cast(IntegerType)
      )

      // 3. Déterminer si les conditions sont IFR (Instrument Flight Rules)
      .withColumn("feature_is_ifr_conditions",
        ((col("feature_visibility_miles") < 3.0) ||
          (col("feature_ceiling") < 1000)).cast(IntegerType)
      )

      // 4. Déterminer si CAT II/III est requis
      .withColumn("feature_requires_cat_ii",
        ((col("feature_visibility_miles") < 1.0) ||
          (col("feature_ceiling") < 200)).cast(IntegerType)
      )

      // 5. Calculer le niveau de risque opérationnel (0-4)
      // 0=None, 1=Low, 2=Moderate, 3=High, 4=Critical
      .withColumn("feature_operations_risk_level",
        when(col("feature_has_obscured") === true ||
          col("feature_visibility_miles") < 0.25 ||
          col("feature_ceiling") < 100, lit(4))  // Critical
          .when(col("feature_visibility_miles") < 0.5 ||
            col("feature_ceiling") < 200, lit(3))  // High
          .when(col("feature_visibility_miles") < 1.0 ||
            col("feature_ceiling") < 500, lit(3))  // High
          .when(col("feature_visibility_miles") < 3.0 ||
            col("feature_ceiling") < 1000, lit(2)) // Moderate
          .when(col("feature_visibility_miles") < 5.0 ||
            col("feature_ceiling") < 3000, lit(1)) // Low
          .otherwise(lit(0))                       // None
      )

      // 6. Flight Category selon les règles officielles FAA
      // Combine visibility ET ceiling height pour déterminer les conditions de vol
      // Règles : utilise OR pour visibilité et plafond (la condition la plus restrictive s'applique)
      .withColumn("feature_flight_category",
        when(
          (col("feature_visibility_miles") < 1.0) ||
          (col("feature_ceiling") < 500),
          lit("LIFR")  // Low Instrument Flight Rules - Conditions les plus sévères
        )
        .when(
          (col("feature_visibility_miles") < 3.0) ||
          (col("feature_ceiling") < 1000),
          lit("IFR")  // Instrument Flight Rules - Conditions sévères
        )
        .when(
          (col("feature_visibility_miles") <= 5.0) ||
          (col("feature_ceiling") <= 3000),
          lit("MVFR")  // Marginal Visual Flight Rules - Conditions modérées
        )
        .otherwise(lit("VFR"))  // Visual Flight Rules - Bonnes conditions
      )

      // 7. Version ordinale de Flight Category pour les modèles ML
      // Plus le score est élevé, plus les conditions sont sévères
      .withColumn("feature_flight_category_ordinal",
        when(col("feature_flight_category") === "LIFR", lit(3))  // Sévérité maximale
          .when(col("feature_flight_category") === "IFR", lit(2))
          .when(col("feature_flight_category") === "MVFR", lit(1))
          .otherwise(lit(0))  // VFR = sévérité minimale
      )

      // 8. Weather_Type basé sur ValueForWindCharacter code
      .withColumn("feature_weather_type",
        coalesce(
          weatherTypeMapCol(col("ValueForWindCharacter").cast("int")),
          lit("Clear_or_Other")
        )
      )

      // 9. Weather_Intensity basé sur ValueForWindCharacter code
      .withColumn("feature_weather_intensity",
        coalesce(
          weatherIntensityMapCol(col("ValueForWindCharacter").cast("int")),
          lit("Not_Applicable")
        )
      )

      // 10. Temp_Dewpoint_Spread - indicateur de brouillard et nuages bas
      // Une valeur faible indique que l'air est saturé (risque de brouillard)
      .withColumn("feature_temp_dewpoint_spread",
        col("DryBulbCelsius") - col("DewPointCelsius")
      )

      // Nettoyer les colonnes temporaires
      .drop("_temp_base_score", "_temp_ceiling_penalty", "_temp_visibility_penalty")
  }
}
