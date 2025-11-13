package com.flightdelay.config

/**
 * Niveaux de log disponibles pour l'application
 * Hiérarchie: DEBUG < INFO < WARN < ERROR
 */
object LogLevel extends Enumeration {
  type LogLevel = Value

  val DEBUG = Value("debug")
  val INFO = Value("info")
  val WARN = Value("warn")
  val ERROR = Value("error")

  /**
   * Parse une chaîne en LogLevel
   * @param level Niveau de log sous forme de chaîne
   * @return LogLevel correspondant
   */
  def fromString(level: String): LogLevel = {
    level.toLowerCase match {
      case "debug" => DEBUG
      case "info" => INFO
      case "warn" => WARN
      case "error" => ERROR
      case _ => INFO // Par défaut
    }
  }

  /**
   * Vérifie si un niveau de log doit être affiché selon le niveau configuré
   * @param messageLevel Niveau du message à afficher
   * @param configuredLevel Niveau configuré dans l'application
   * @return true si le message doit être affiché
   */
  def shouldLog(messageLevel: LogLevel, configuredLevel: LogLevel): Boolean = {
    messageLevel.id >= configuredLevel.id
  }
}
