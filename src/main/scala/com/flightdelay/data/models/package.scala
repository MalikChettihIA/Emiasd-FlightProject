package com.flightdelay.data

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.{Try, Success, Failure}

package object model {

  // Types aliases pour améliorer la lisibilité
  type AirportCode = String
  type CarrierCode = String
  type FlightNumber = String
  type StationCode = String

  // Constantes communes
  val DOT_DELAY_THRESHOLD = 15 // minutes
  val AIRPORT_CODE_LENGTH = 3

  // Formatters de dates réutilisables
  val FLIGHT_DATE_FORMAT: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val TIMESTAMP_FORMAT: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  val COMPOSITE_ID_FORMAT: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

  // Regex patterns
  val AIRPORT_CODE_PATTERN = "^[A-Z]{3}$".r
  val CARRIER_CODE_PATTERN = "^[A-Z0-9]{2,3}$".r

  // Constantes météorologiques
  val VALID_QUALITY_FLAGS = Set("0", "1", "4", "5", "9")
  val MISSING_VALUE_INDICATORS = Set("999.9", "9999", "-9999", "")
}