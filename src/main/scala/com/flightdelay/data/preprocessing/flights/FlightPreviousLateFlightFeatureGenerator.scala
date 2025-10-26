package com.flightdelay.data.preprocessing.flights

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame

object FlightPreviousLateFlightFeatureGenerator {
  /**
   * Crée la feature 'Late Aircraft' qui capture le retard de l'avion
   * sur son vol précédent de la journée.
   *
   * Crée aussi la feature 'Turnaround Buffer' qui représente le temps réel
   * disponible entre l'arrivée effective du vol précédent et le départ prévu du vol actuel.
   *
   * @param df DataFrame contenant les données de vols
   * @return DataFrame enrichi avec PREV_AIRCRAFT_ARR_DELAY et feature_turnaround_buffer
   */
  def createLateAircraftFeature(df: DataFrame): DataFrame = {

    println("\n[PreviousLateFlightFeature] Creating PREV_AIRCRAFT_ARR_DELAY and turnaround_buffer features...")

    // Étape 1: Créer un identifiant d'avion proxy
    // En l'absence de TAIL_NUM, on combine compagnie + numéro de vol
    val dfWithAircraftId = df.withColumn(
      "AIRCRAFT_ID",
      concat(
        col("OP_CARRIER_AIRLINE_ID").cast("string"),
        lit("_"),
        col("OP_CARRIER_FL_NUM").cast("string")
      )
    )

    // ✅ OPTIMIZATION: Repartitionner explicitement avant la window function
    // Cela améliore les performances en réduisant le shuffle
    println(s"  - Repartitioning by AIRCRAFT_ID and FL_DATE...")
    val dfRepartitioned = dfWithAircraftId
      .repartition(200, col("AIRCRAFT_ID"), col("FL_DATE"))  // 200 partitions
      .cache()  // Cache pour éviter de recalculer

    val countBefore = dfRepartitioned.count()
    println(f"  - Processing $countBefore%,d flights")

    // Étape 2: Définir la fenêtre de partitionnement et tri
    // On partitionne par avion ET date, puis on trie par heure de départ prévue
    val windowSpec = Window
      .partitionBy("AIRCRAFT_ID", "FL_DATE")
      .orderBy("CRS_DEP_TIME")

    // Étape 3: Utiliser lag() pour récupérer les infos du vol précédent
    // lag() est l'équivalent Spark de shift() en pandas
    println(s"  - Applying window function to compute previous flight features...")
    val dfWithPrevDelay = dfRepartitioned
      .withColumn(
        "PREV_AIRCRAFT_ARR_DELAY",
        lag("ARR_DELAY_NEW", 1).over(windowSpec)
      )
      .withColumn(
        "_prev_crs_arr_time",
        lag("CRS_ARR_TIME", 1).over(windowSpec)
      )

    // Étape 4: Calculer le Turnaround Buffer
    // Turnaround Buffer = temps réel disponible entre l'arrivée effective du vol précédent
    // et le départ prévu du vol actuel
    println(s"  - Computing turnaround_buffer...")

    val dfWithTurnaround = dfWithPrevDelay
      // Convertir CRS_ARR_TIME du vol précédent en minutes depuis minuit
      .withColumn("_prev_arr_minutes",
        when(col("_prev_crs_arr_time").isNotNull,
          (col("_prev_crs_arr_time") / lit(100)) * lit(60) + (col("_prev_crs_arr_time") % lit(100))
        ).otherwise(null)
      )
      // Ajouter le retard à l'arrivée (en minutes) pour obtenir l'heure d'arrivée RÉELLE
      .withColumn("_prev_actual_arr_minutes",
        when(col("_prev_arr_minutes").isNotNull && col("PREV_AIRCRAFT_ARR_DELAY").isNotNull,
          col("_prev_arr_minutes") + col("PREV_AIRCRAFT_ARR_DELAY")
        ).otherwise(col("_prev_arr_minutes"))
      )
      // Convertir CRS_DEP_TIME actuel en minutes depuis minuit
      .withColumn("_curr_dep_minutes",
        (col("CRS_DEP_TIME") / lit(100)) * lit(60) + (col("CRS_DEP_TIME") % lit(100))
      )
      // Calculer le turnaround buffer en minutes
      // Gérer le cas où le vol traverse minuit (ajout de 1440 minutes = 24h)
      .withColumn("feature_turnaround_buffer",
        when(col("_prev_actual_arr_minutes").isNotNull,
          when(col("_curr_dep_minutes") >= col("_prev_actual_arr_minutes"),
            col("_curr_dep_minutes") - col("_prev_actual_arr_minutes")
          ).otherwise(
            // Si négatif, cela signifie que le vol traverse minuit
            col("_curr_dep_minutes") + lit(1440) - col("_prev_actual_arr_minutes")
          )
        ).otherwise(null)  // Pas de vol précédent
      )

    // Étape 5: Remplacer les valeurs null par des valeurs par défaut
    // Les valeurs null correspondent au premier vol de la journée pour cet avion
    val dfFinal = dfWithTurnaround
      .withColumn(
        "PREV_AIRCRAFT_ARR_DELAY",
        coalesce(col("PREV_AIRCRAFT_ARR_DELAY"), lit(0.0))
      )
      .withColumn(
        "feature_turnaround_buffer",
        coalesce(col("feature_turnaround_buffer"), lit(0.0))
      )

    // Étape 6: Créer des features dérivées
    val dfWithDerivedFeatures = dfFinal
      // Feature binaire: avion précédent en retard
      .withColumn(
        "IS_PREV_AIRCRAFT_LATE",
        when(col("PREV_AIRCRAFT_ARR_DELAY") > 0, 1).otherwise(0)
      )
      // Feature binaire: turnaround serré (< 30 minutes)
      .withColumn(
        "feature_is_tight_turnaround",
        when(col("feature_turnaround_buffer") < 30, 1).otherwise(0)
      )
      // Feature binaire: turnaround très serré (< 15 minutes)
      .withColumn(
        "feature_is_very_tight_turnaround",
        when(col("feature_turnaround_buffer") < 15, 1).otherwise(0)
      )
      // Catégorisation du turnaround
      .withColumn(
        "feature_turnaround_category",
        when(col("feature_turnaround_buffer") < 15, lit("very_tight"))
          .when(col("feature_turnaround_buffer") < 30, lit("tight"))
          .when(col("feature_turnaround_buffer") < 60, lit("normal"))
          .when(col("feature_turnaround_buffer") < 120, lit("comfortable"))
          .otherwise(lit("very_comfortable"))
      )

    // Nettoyer les colonnes temporaires
    val dfClean = dfWithDerivedFeatures.drop(
      "_prev_crs_arr_time",
      "_prev_arr_minutes",
      "_prev_actual_arr_minutes",
      "_curr_dep_minutes"
    )

    // Unpersist le cache pour libérer la mémoire
    dfRepartitioned.unpersist()

    println(s"  ✓ PREV_AIRCRAFT_ARR_DELAY and turnaround_buffer features created successfully")

    dfClean
  }

}
