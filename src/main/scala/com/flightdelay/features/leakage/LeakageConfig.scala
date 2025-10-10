package com.flightdelay.features.leakage

/**
 * Configuration des règles de détection et suppression du data leakage
 */
object LeakageConfig {

  /**
   * Colonnes sources de data leakage (données connues APRÈS le vol)
   * Ces colonnes doivent être supprimées lors du preprocessing
   */
  val SOURCE_LEAKAGE_COLUMNS: Seq[String] = Seq(
    "ARR_DELAY_NEW",    // Retard réel à l'arrivée - C'EST LA CIBLE !
    "WEATHER_DELAY",    // Retard météo (connu APRÈS le vol)
    "NAS_DELAY"         // Retard NAS (connu APRÈS le vol)
  )

  /**
   * Préfixe des colonnes de labels générées
   */
  val LABEL_PREFIX: String = "label_"

  /**
   * Préfixe des colonnes de features générées
   */
  val FEATURE_PREFIX: String = "feature_"

  /**
   * Colonnes qui ne doivent jamais être utilisées comme features
   * (même si elles ne causent pas de leakage direct)
   */
  val FORBIDDEN_FEATURE_COLUMNS: Seq[String] = Seq(
    "FL_DATE",                    // Date du vol (identifiant temporel)
    "OP_CARRIER_FL_NUM",         // Numéro de vol (identifiant)
    "feature_flight_unique_id",  // ID unique du vol
    "feature_flight_timestamp"   // Timestamp (redondant avec la date)
  )

}
