package com.flightdelay.features.joiners

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object FlightWeatherDataJoiner {

  /**
   * Jointure spatio-temporelle entre vols et observations météo pour ORIGINE et DESTINATION
   *
   * @param flightDF DataFrame des vols
   * @param weatherDF DataFrame météo
   * @param weatherDepthHours Nombre d'heures d'observations météo à récupérer avant le vol (par défaut: 12)
   * @param removeLeakageColumns Si true, supprime automatiquement les colonnes qui causent du data leakage (par défaut: false)
   * @param selectedFlightColumns Colonnes du DataFrame vols à conserver (si None, toutes les colonnes)
   * @param selectedWeatherColumns Colonnes du DataFrame météo à inclure dans les observations (si None, toutes les colonnes)
   * @return DataFrame avec origin_weather_observations et dest_weather_observations
   */
  def joinFlightsWithWeather(
                              flightDF: DataFrame,
                              weatherDF: DataFrame,
                              weatherDepthHours: Int = 12,
                              removeLeakageColumns: Boolean = true,
                              selectedFlightColumns: Option[Seq[String]] = None,
                              selectedWeatherColumns: Option[Seq[String]] = None
                            ): DataFrame = {

    require(weatherDepthHours > 0 && weatherDepthHours <= 24,
      s"weatherDepthHours doit être entre 1 et 24, valeur fournie: $weatherDepthHours")

    // Première jointure : aéroport d'origine
    val withOriginWeather = joinWeatherForAirport(
      flightDF,
      weatherDF,
      "origin",
      weatherDepthHours,
      selectedFlightColumns,
      selectedWeatherColumns
    )

    // Deuxième jointure : aéroport de destination
    val withBothWeather = joinWeatherForAirport(
      withOriginWeather,
      weatherDF,
      "destination",
      weatherDepthHours,
      selectedFlightColumns,
      selectedWeatherColumns
    )

    // Supprimer les colonnes de leakage si demandé
    if (removeLeakageColumns) {
      removeDataLeakageColumns(
        withBothWeather,
        keepRelativeTime = removeLeakageColumns,  // ou true si tu veux garder hours_before_flight
        selectedWeatherColumns = selectedWeatherColumns
      )
    } else {
      withBothWeather
    }
  }

  /**
   * Supprime les colonnes qui peuvent causer du data leakage
   *
   * @param df DataFrame avec potentiellement des colonnes de leakage
   * @param keepRelativeTime Si true, transforme Time en hours_before_flight. Si false, supprime Time/Date complètement
   * @param selectedWeatherColumns Les colonnes météo qui ont été sélectionnées lors de la jointure
   * @return DataFrame nettoyé sans colonnes de leakage
   */
  def removeDataLeakageColumns(
                                df: DataFrame,
                                keepRelativeTime: Boolean = false,
                                selectedWeatherColumns: Option[Seq[String]] = None
                              ): DataFrame = {

    val leakagePatterns = Set(
      "DEP_TIME",
      "DEP_DELAY",
      "DEP_DELAY_NEW",
      "ARR_TIME",
      "ARR_DELAY",
      "ARR_DELAY_NEW",
      "CARRIER_DELAY",
      "WEATHER_DELAY",
      "NAS_DELAY",
      "SECURITY_DELAY",
      "LATE_AIRCRAFT_DELAY",
      "ACTUAL_ELAPSED_TIME",
      "AIR_TIME",
      "CANCELLED",
      "CANCELLATION_CODE",
      "DIVERTED",
      "DISTANCE",
      "feature_arrival_hour_rounded",
      "feature_utc_arrival_hour_rounded",
      "feature_arrival_hour",
      "feature_utc_arrival_hour",
      "UTC_ARR_TIME",
      "UTC_ARR_DATE"
    )

    val currentColumns = df.columns.toSet
    val columnsToRemove = currentColumns.intersect(leakagePatterns)
    val columnsToKeep = currentColumns.diff(columnsToRemove)

    if (columnsToRemove.nonEmpty) {
      println(s"[Anti-Leakage] Colonnes supprimées : ${columnsToRemove.toSeq.sorted.mkString(", ")}")
    }

    val dfCleaned = df.select(columnsToKeep.toSeq.sorted.map(col): _*)

    // Déterminer les colonnes météo à garder (sans WBAN, Date, Time qui sont obligatoires)
    val weatherColsToKeep = selectedWeatherColumns match {
      case Some(cols) =>
        // Filtrer pour ne garder que les colonnes demandées (sans les colonnes techniques)
        cols.filterNot(c => c == "WBAN" || c == "Date" || c == "Time")
      case None =>
        // Si None, on doit inférer les colonnes à partir du premier élément du premier array
        // Pour l'instant, on va juste supprimer Time et Date, et garder le reste
        Seq() // On ne peut pas le savoir sans inspecter le DataFrame
    }

    // Nettoyer les observations météo
    val weatherObsCols = dfCleaned.columns.filter(_.endsWith("_weather_observations"))

    var result = dfCleaned

    if (weatherObsCols.nonEmpty) {
      // Construire dynamiquement le struct en fonction des colonnes sélectionnées
      val structFields = if (weatherColsToKeep.nonEmpty) {
        // Utiliser les colonnes explicitement demandées
        weatherColsToKeep.map(colName => s"obs.$colName as $colName").mkString(", ")
      } else {
        // Mode fallback : garder toutes les colonnes sauf WBAN, Date, Time
        // On va devoir inspecter le schema du premier array
        val sampleRow = dfCleaned.select(weatherObsCols.head).first()
        val weatherArray = sampleRow.getAs[Seq[_]](0)

        if (weatherArray.nonEmpty) {
          val firstObs = weatherArray.head.asInstanceOf[org.apache.spark.sql.Row]
          val schema = firstObs.schema

          schema.fields
            .map(_.name)
            .filterNot(name => name == "WBAN" || name == "Date" || name == "Time")
            .map(colName => s"obs.$colName as $colName")
            .mkString(", ")
        } else {
          // Array vide, on ne peut rien faire
          ""
        }
      }

      if (keepRelativeTime && structFields.nonEmpty) {
        // Garder Time mais le transformer en index relatif
        weatherObsCols.foreach { weatherCol =>
          result = result.withColumn(
            weatherCol,
            expr(s"""
                transform(
                  $weatherCol,
                  obs -> struct(
                    $structFields,
                    cast((obs.Time - element_at($weatherCol, -1).Time) / 100 as int) as hours_before_flight
                  )
                )
              """)
          )
        }
        println(s"[Anti-Leakage] Time transformé en hours_before_flight (index relatif)")
        println(s"[Anti-Leakage] Champs conservés dans observations météo : ${weatherColsToKeep.mkString(", ")}, hours_before_flight")

      } else if (structFields.nonEmpty) {
        // Supprimer complètement Time et Date
        weatherObsCols.foreach { weatherCol =>
          result = result.withColumn(
            weatherCol,
            expr(s"""
                transform(
                  $weatherCol,
                  obs -> struct($structFields)
                )
              """)
          )
        }
        println(s"[Anti-Leakage] Champs supprimés des observations météo : WBAN, Time, Date")
        println(s"[Anti-Leakage] Champs conservés dans observations météo : ${weatherColsToKeep.mkString(", ")}")
      }
    }

    result
  }
  /**
   * Jointure spatio-temporelle pour UN aéroport (origine OU destination)
   */
  def joinWeatherForAirport(
                             flightDF: DataFrame,
                             weatherDF: DataFrame,
                             airportType: String,
                             weatherDepthHours: Int,
                             selectedFlightColumns: Option[Seq[String]] = None,
                             selectedWeatherColumns: Option[Seq[String]] = None
                           ): DataFrame = {

    require(
      airportType == "origin" || airportType == "destination",
      "airportType doit être 'origin' ou 'destination'"
    )

    require(weatherDepthHours > 0 && weatherDepthHours <= 24,
      s"weatherDepthHours doit être entre 1 et 24, valeur fournie: $weatherDepthHours")

    import flightDF.sparkSession.implicits._

    val isOrigin = airportType == "origin"

    // Déterminer les colonnes à utiliser selon le type d'aéroport
    val (wbanCol, dateCol, timeRoundedCol) = if (isOrigin) {
      ("ORIGIN_WBAN", "UTC_FL_DATE", "feature_utc_departure_hour_rounded")
    } else {
      ("DEST_WBAN", "UTC_ARR_DATE", "feature_utc_arrival_hour_rounded")
    }

    // Déterminer les colonnes météo à inclure dans le struct
    val weatherColsToKeep = selectedWeatherColumns match {
      case Some(cols) =>
        val mandatory = Seq("WBAN", "Date", "Time")
        (mandatory ++ cols).distinct
      case None =>
        weatherDF.columns.toSeq
    }

    // ============================================
    // PHASE MAP - Préparation des données
    // ============================================

    val weatherTagged = weatherDF
      .withColumn("Time", col("Time").cast("int"))
      .withColumn("table_tag", lit("OT"))
      .withColumn("join_key_wban", col("WBAN"))
      .withColumn("join_key_date", col("Date"))

    val flightTagged = flightDF
      .filter(col(wbanCol).isNotNull && col(timeRoundedCol).isNotNull)
      .withColumn("table_tag", lit("FT"))
      .withColumn("join_key_wban", col(wbanCol))
      .withColumn("join_key_date", col(dateCol))
      .withColumn("time_rounded", col(timeRoundedCol))

    val flightsNeedingDuplication = flightTagged
      .filter(col("time_rounded") / 100 < weatherDepthHours)
      .withColumn("join_key_date", date_sub(col("join_key_date"), 1))

    val flightComplete = flightTagged.union(flightsNeedingDuplication)

    // ============================================
    // PHASE SHUFFLE - Préparation pour jointure
    // ============================================

    val weatherForJoin = weatherTagged
      .select(
        Seq(
          col("join_key_wban").as("key_wban"),
          col("join_key_date").as("key_date"),
          col("table_tag")
        ) ++ weatherColsToKeep.map(c => col(c)): _*
      )

    val allFlightCols = flightComplete.columns.toSeq
    val excludeCols = Set("table_tag", "join_key_wban", "join_key_date", "time_rounded")
    val flightDataCols = allFlightCols.filterNot(excludeCols.contains)

    val flightForJoin = flightComplete
      .select(
        Seq(
          col("join_key_wban").as("key_wban"),
          col("join_key_date").as("key_date"),
          col("time_rounded")
        ) ++ flightDataCols.map(c => col(c)): _*
      )

    // ============================================
    // PHASE REDUCE - Jointure et Agrégation
    // ============================================

    val weatherStructCols = weatherColsToKeep.map(c => col(c))

    val weatherGrouped = weatherForJoin
      .groupBy("key_wban", "key_date")
      .agg(
        collect_list(
          struct(weatherStructCols: _*)
        ).as("all_weather_obs")
      )

    val joined = flightForJoin
      .join(
        weatherGrouped,
        flightForJoin("key_wban") === weatherGrouped("key_wban") &&
          flightForJoin("key_date") === weatherGrouped("key_date"),
        "left"
      )

    val weatherColName = s"${airportType}_weather_observations"

    val joinedWithFiltered = joined
      .withColumn("required_hours",
        expr(s"""
          transform(
            sequence(0, $weatherDepthHours),
            i -> cast((time_rounded / 100 - ($weatherDepthHours - i) + 24) % 24 * 100 as int)
          )
        """)
      )
      .withColumn(weatherColName,
        expr("""
          filter(
            all_weather_obs,
            obs -> array_contains(required_hours, obs.Time)
          )
        """)
      )

    // ============================================
    // SÉLECTION FINALE DES COLONNES
    // ============================================

    val finalFlightCols = selectedFlightColumns match {
      case Some(cols) =>
        val colsSet = cols.toSet

        val requiredCols = Set(
          "ORIGIN_WBAN", "UTC_FL_DATE", "feature_utc_departure_hour_rounded",
          "DEST_WBAN", "UTC_ARR_DATE", "feature_utc_arrival_hour_rounded"
        )

        val missingRequired = requiredCols
          .filter(flightDataCols.contains)
          .diff(colsSet)

        var finalCols = (cols ++ missingRequired).distinct

        if (!isOrigin &&
          flightDataCols.contains("origin_weather_observations") &&
          !colsSet.contains("origin_weather_observations")) {
          finalCols = (finalCols :+ "origin_weather_observations").distinct
        }

        if (missingRequired.nonEmpty) {
          println(s"[FlightWeatherJoinner] Colonnes ajoutées automatiquement pour $airportType: ${missingRequired.mkString(", ")}")
        }

        finalCols

      case None =>
        flightDataCols
    }

    val finalColsSeq = (finalFlightCols :+ weatherColName).map(c => col(c))

    val result = joinedWithFiltered
      .select(finalColsSeq: _*)
      .filter(size(col(weatherColName)) > 0)

    result
  }
}