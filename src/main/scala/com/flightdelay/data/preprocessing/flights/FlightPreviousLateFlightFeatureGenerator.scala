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
   * @param df DataFrame contenant les données de vols
   * @return DataFrame enrichi avec la colonne PREV_AIRCRAFT_ARR_DELAY
   */
  def createLateAircraftFeature(df: DataFrame): DataFrame = {

    println("\n[PreviousLateFlightFeature] Creating PREV_AIRCRAFT_ARR_DELAY feature...")

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

    // Étape 3: Utiliser lag() pour récupérer le retard du vol précédent
    // lag() est l'équivalent Spark de shift() en pandas
    println(s"  - Applying window function to compute previous flight delay...")
    val dfWithPrevDelay = dfRepartitioned.withColumn(
      "PREV_AIRCRAFT_ARR_DELAY",
      lag("ARR_DELAY_NEW", 1).over(windowSpec)
    )

    // Étape 4: Remplacer les valeurs null par 0
    // Les valeurs null correspondent au premier vol de la journée pour cet avion
    val dfFinal = dfWithPrevDelay.withColumn(
      "PREV_AIRCRAFT_ARR_DELAY",
      coalesce(col("PREV_AIRCRAFT_ARR_DELAY"), lit(0.0))
    )

    // Étape 5 (optionnelle): Créer une feature binaire indiquant si l'avion était en retard
    val dfWithBinaryFeature = dfFinal.withColumn(
      "IS_PREV_AIRCRAFT_LATE",
      when(col("PREV_AIRCRAFT_ARR_DELAY") > 0, 1).otherwise(0)
    )

    // Unpersist le cache pour libérer la mémoire
    dfRepartitioned.unpersist()

    println(s"  ✓ PREV_AIRCRAFT_ARR_DELAY feature created successfully")

    dfWithBinaryFeature
  }

}
