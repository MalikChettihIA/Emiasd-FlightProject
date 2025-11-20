package com.flightdelay.features.joiners

import com.flightdelay.config.AppConfiguration
import org.apache.spark.sql.{DataFrame, Column, Row, SparkSession}
import org.apache.spark.sql.functions._
import com.flightdelay.utils.DebugUtils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object FlightWeatherDataJoiner {

  /**
   * Jointure spatio-temporelle entre vols et observations météo pour ORIGINE et DESTINATION
   *
   * @param flightDF DataFrame des vols
   * @param weatherDF DataFrame météo
   * @param weatherOriginDepthHours Nombre d'heures d'observations météo à récupérer avant le vol
   *                                - Valeur négative : pas de jointure météo pour l'origine
   *                                - 0 : jointure à l'heure exacte du départ
   *                                - >0 : jointure avec historique (max 11)
   * @param weatherDestinationDepthHours Nombre d'heures d'observations météo à récupérer avant le vol
   *                                     - Valeur négative : pas de jointure météo pour la destination
   *                                     - 0 : jointure à l'heure exacte de l'arrivée
   *                                     - >0 : jointure avec historique (max 11)
   * @param removeLeakageColumns Si true, supprime automatiquement les colonnes qui causent du data leakage (par défaut: false)
   * @param selectedFlightColumns Colonnes du DataFrame vols à conserver (si None, toutes les colonnes)
   * @param selectedWeatherColumns Colonnes du DataFrame météo à inclure dans les observations (si None, toutes les colonnes)
   * @return DataFrame avec origin_weather_observations et dest_weather_observations
   */
  def joinFlightsWithWeather(
                              flightDF: DataFrame,
                              weatherDF: DataFrame,
                              weatherOriginDepthHours: Int = 0,
                              weatherDestinationDepthHours: Int = 0,
                              removeLeakageColumns: Boolean = true,
                              selectedFlightColumns: Option[Seq[String]] = None,
                              selectedWeatherColumns: Option[Seq[String]] = None
                            )(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {


    // Validation : valeurs négatives acceptées (= pas de jointure), sinon max 11
    require(weatherOriginDepthHours <= 11,
      s"weatherOriginDepthHours doit être <= 11 (ou négatif pour désactiver), valeur fournie: $weatherOriginDepthHours")

    require(weatherDestinationDepthHours <= 11,
      s"weatherDestinationDepthHours doit être <= 11 (ou négatif pour désactiver), valeur fournie: $weatherDestinationDepthHours")

    // Si les deux sont négatifs, pas de jointure du tout
    if (weatherOriginDepthHours < 0 && weatherDestinationDepthHours < 0) {
      info("[FlightWeatherJoiner]   Both weatherOriginDepthHours and weatherDestinationDepthHours are negative - NO weather join")
      return flightDF
    }

    // Paramètres de partitions "reducers"
    val cores = spark.sparkContext.defaultParallelism

    val numPartsOrigin = pickParts(3.3, 32, 128, cores) // ≈ 40
    val numPartsDest   = pickParts(5.2, 48, 192, cores) // ≈ 64
    spark.conf.set("spark.sql.shuffle.partitions", numPartsDest) // borne haute DF

    // Préparation de WeatherDF
    val weather = weatherDF.select(
      col("Date").cast(DateType).as("WDATE"),
      col("Time").as("WTIME_HHMM"),
      col("*")
    ).where(col("WBAN").isNotNull && length(col("WBAN")) > 0 && col("WDATE").isNotNull)

    // ------------------------
    // Préparation météo avec relHour + duplication J/J+1 (Map)
    // ------------------------
    val weatherWithHour = weather
      .withColumn("hour", hhmmHourCol(col("WTIME_HHMM")))
      .na.fill(Map("hour" -> -1))

    val meteoSameDay = weatherWithHour
      .withColumn("relHour", col("hour"))
      .withColumn("DATE", col("WDATE"))

    val meteoNextDay = weatherWithHour
      .withColumn("relHour", col("hour") - lit(24))
      .withColumn("DATE", date_add(col("WDATE"), 1))

    val weatherRel = meteoSameDay.unionByName(meteoNextDay)
      .filter(col("relHour").between(-24, 23))

    // ------------------------
    // Reduce météo par clé → Map relHour -> struct (Reduce)
    // ------------------------

    // 1) Récupérer les features météo depuis la config (Option[Map[...]])


    // 2) Colonnes fixes dans la struct
    val staticCols = Seq(
      col("relHour").as("hour"),
      col("WBAN"),
      col("WDATE"),
      col("WTIME_HHMM")
    )

    // 3) Colonnes dynamiques venant de la config
    val staticNames = Set("relHour", "WBAN", "WDATE", "WTIME_HHMM")
    val allWeatherCols: Seq[String] =
      selectedWeatherColumns.getOrElse(Seq.empty[String])
    val dynamicFeatureCols =
      allWeatherCols
        .filterNot(staticNames.contains)
        .map(c => col(c).alias(c))

    // 4) Struct finale : [hour, WBAN, WDATE, WTIME_HHMM, <features>...]
    val weatherStruct =
      struct((staticCols ++ dynamicFeatureCols): _*)

    // 5) Agrégation en map relHour -> struct(...)
    val weatherByKey: DataFrame =
      weatherRel
        .groupBy(col("WBAN"), col("DATE"))
        .agg(
          map_from_entries(
            collect_list(struct(col("relHour"), weatherStruct))
          ).as("wmap")
        )

    // Première jointure : aéroport d'origine (seulement si >= 0)
    val withOriginWeather = if (weatherOriginDepthHours >= 0) {
      // ------------------------
      // JOIN #1 — ORIGIN (Partition = hash(ORIGIN_WBAN, UTC_FL_DATE))
      // ------------------------
      val flightsDep = flightDF
        .withColumn("depHour", coalesce(hhmmHourCol(col("UTC_CRS_DEP_TIME")), lit(0)))

      val originPre = flightsDep
        .repartition(numPartsOrigin, col("ORIGIN_WBAN"), col("UTC_FL_DATE")) // <-- Hash partition explicite
        .join(
          weatherByKey.hint("shuffle_hash"),
          col("ORIGIN_WBAN") === weatherByKey("WBAN") &&
            col("UTC_FL_DATE")    === weatherByKey("DATE"),
          "left"
        )
        .drop(weatherByKey("WBAN")).drop(weatherByKey("DATE"))

      val originWithWoArr = originPre
        .withColumn("Wo", expr(s"transform(sequence(1, ${weatherOriginDepthHours}), i -> element_at(wmap, depHour - i))"))
        .drop("wmap")

      val woCols = (0 until weatherOriginDepthHours).map(i => col("Wo").getItem(i).as(s"Wo_h${i+1}"))
      val originDF = originWithWoArr
        .select(col("*") +: woCols: _*)
        .drop("Wo")
        .persist()

      originDF
    } else {
      info(s"[FlightWeatherJoiner] Skipping origin weather join (weatherOriginDepthHours=$weatherOriginDepthHours)")
      flightDF
    }

    // Deuxième jointure : aéroport de destination (seulement si >= 0)
    val withBothWeather = if (weatherDestinationDepthHours >= 0) {
      val flightsArr = withOriginWeather
        .withColumn("arrHour", coalesce(hhmmHourCol(col("UTC_ARR_TIME")), lit(0)))

      val destPre = flightsArr
        .repartition(numPartsDest, col("DEST_WBAN"), col("UTC_ARR_DATE"))     // <-- Hash partition explicite
        .join(
          weatherByKey.hint("shuffle_hash"),
          col("DEST_WBAN") === weatherByKey("WBAN") &&
            col("UTC_ARR_DATE")  === weatherByKey("DATE"),
          "left"
        )
        .drop(weatherByKey("WBAN")).drop(weatherByKey("DATE"))

      val destWithWdArr = destPre
        .withColumn("Wd", expr(s"transform(sequence(1, ${weatherDestinationDepthHours}), i -> element_at(wmap, arrHour - i))"))
        .drop("wmap")

      val wdCols = (0 until weatherDestinationDepthHours).map(i => col("Wd").getItem(i).as(s"Wd_h${i+1}"))
      
      val baseCols: Seq[org.apache.spark.sql.Column] =
        withOriginWeather.columns.map(col).toSeq

      val joinedDF = destWithWdArr
        .select( (baseCols ++ wdCols): _* )
        .drop("Wd")
        .persist()

      joinedDF
    } else {
      info(s"[FlightWeatherJoiner] Skipping destination weather join (weatherDestinationDepthHours=$weatherDestinationDepthHours)")
      withOriginWeather
    }

    // Supprimer les colonnes météo non désirées basées sur les valeurs négatives
    var finalResult = withBothWeather

    // Si weatherOriginDepthHours < 0, supprimer origin_weather_observations si elle existe
    if (weatherOriginDepthHours < 0 && finalResult.columns.contains("origin_weather_observations")) {
      info(s"[FlightWeatherJoiner] Removing origin_weather_observations column (weatherOriginDepthHours=$weatherOriginDepthHours)")
      finalResult = finalResult.drop("origin_weather_observations")
    }

    // Si weatherDestinationDepthHours < 0, supprimer destination_weather_observations si elle existe
    if (weatherDestinationDepthHours < 0 && finalResult.columns.contains("destination_weather_observations")) {
      info(s"[FlightWeatherJoiner] Removing destination_weather_observations column (weatherDestinationDepthHours=$weatherDestinationDepthHours)")
      finalResult = finalResult.drop("destination_weather_observations")
    }

    // Supprimer les colonnes de leakage si demandé
    if (removeLeakageColumns) {
      removeDataLeakageColumns(
        finalResult,
        keepRelativeTime = removeLeakageColumns,  // ou true si tu veux garder hours_before_flight
        selectedWeatherColumns = selectedWeatherColumns
      )
    } else {
      finalResult
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
                              )(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    val leakagePatterns = Set(
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
      info(s"[Anti-Leakage] Colonnes supprimees : ${columnsToRemove.toSeq.sorted.mkString(", ")}")
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
        info(s"[Anti-Leakage] Time transforme en hours_before_flight (index relatif)")
        info(s"[Anti-Leakage] Champs conserves dans observations meteo : ${weatherColsToKeep.mkString(", ")}, hours_before_flight")

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
        info(s"[Anti-Leakage] Champs supprimés des observations météo : WBAN, Time, Date")
        info(s"[Anti-Leakage] Champs conservés dans observations météo : ${weatherColsToKeep.mkString(", ")}")
      }
    }

    result
  }

  def pickParts(mult: Double, minAbs: Int, maxAbs: Int, cores: Int): Int =
    math.min(maxAbs, math.max(minAbs, math.round(cores * mult).toInt))

  // ------------------------
  // Utilitaire HHMM -> hour [0..23] (sans UDF) (Map)
  // ------------------------
  def hhmmHourCol(c: Column): Column = {
    val s  = regexp_replace(c.cast("string"), ":", "")
    val p4 = lpad(s, 4, "0")
    (substring(p4, 1, 2).cast("int") % 24)
  }
}