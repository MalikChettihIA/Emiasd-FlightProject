package com.flightdelay.utils

import com.flightdelay.config.{AppConfiguration, LogLevel}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Utilitaires pour le logging de l'application
 * Permet de contrôler l'affichage des messages via la configuration (log: true/false, logLevel: debug/info/warn/error)
 */
object DebugUtils {

  // Date formatter for logs (yy/MM/dd HH:mm:ss format)
  private val dateFormatter = DateTimeFormatter.ofPattern("yy/MM/dd")
  private val timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss")

  // Cache the date string to avoid recalculating it on every log
  private var cachedDateString: String = LocalDateTime.now().format(dateFormatter)
  private var lastCheckDay: Int = LocalDateTime.now().getDayOfYear

  /**
   * Get current timestamp for logs in format: yy/MM/dd HH:mm:ss
   * Date is cached and only recalculated when day changes
   */
  private def getTimestamp(): String = {
    val now = LocalDateTime.now()
    val currentDay = now.getDayOfYear

    // Update cached date only if day has changed
    if (currentDay != lastCheckDay) {
      cachedDateString = now.format(dateFormatter)
      lastCheckDay = currentDay
    }

    s"$cachedDateString ${now.format(timeFormatter)}"
  }

  /**
   * Vérifie si un message doit être loggé selon la configuration
   * @param messageLevel Niveau du message
   * @param configuration Configuration de l'application
   * @return true si le message doit être affiché
   */
  private def shouldLog(messageLevel: LogLevel.LogLevel)(implicit configuration: AppConfiguration): Boolean = {
    configuration.common.log && LogLevel.shouldLog(messageLevel, configuration.common.getLogLevel)
  }

  /**
   * Affiche un message de niveau DEBUG
   * @param msg Message à afficher (évalué uniquement si le niveau le permet)
   * @param configuration Configuration de l'application
   */
  def debug(msg: => String)(implicit configuration: AppConfiguration): Unit = {
    if (shouldLog(LogLevel.DEBUG)) {
      println(s"[DEBUG] ${getTimestamp()} $msg")
    }
  }

  /**
   * Affiche un message de niveau INFO
   * @param msg Message à afficher
   * @param configuration Configuration de l'application
   */
  def info(msg: => String)(implicit configuration: AppConfiguration): Unit = {
    if (shouldLog(LogLevel.INFO)) {
      println(s"[INFO] ${getTimestamp()} $msg")
    }
  }

  /**
   * Affiche un message de niveau WARN
   * @param msg Message à afficher
   * @param configuration Configuration de l'application
   */
  def warn(msg: => String)(implicit configuration: AppConfiguration): Unit = {
    if (shouldLog(LogLevel.WARN)) {
      println(s"[WARN] ${getTimestamp()} $msg")
    }
  }

  /**
   * Affiche un message de niveau ERROR
   * @param msg Message à afficher
   * @param configuration Configuration de l'application
   */
  def error(msg: => String)(implicit configuration: AppConfiguration): Unit = {
    if (shouldLog(LogLevel.ERROR)) {
      println(s"[ERROR] ${getTimestamp()} $msg")
    }
  }

  /**
   * Affiche un message de niveau DEBUG sans préfixe
   * Utile pour remplacer les println existants
   * @param msg Message à afficher
   * @param configuration Configuration de l'application
   */
  def debugPrintln(msg: => String)(implicit configuration: AppConfiguration): Unit = {
    if (shouldLog(LogLevel.DEBUG)) {
      println(msg)
    }
  }

  /**
   * Affiche plusieurs messages de niveau DEBUG
   * @param messages Messages à afficher
   * @param configuration Configuration de l'application
   */
  def debugPrintln(messages: Seq[String])(implicit configuration: AppConfiguration): Unit = {
    if (shouldLog(LogLevel.DEBUG)) {
      messages.foreach(println)
    }
  }

  /**
   * Exécute un bloc de code uniquement si le niveau DEBUG est actif
   * Utile pour des opérations de debug coûteuses
   * @param block Bloc de code à exécuter
   * @param configuration Configuration de l'application
   */
  def whenDebug[T](block: => T)(implicit configuration: AppConfiguration): Option[T] = {
    if (shouldLog(LogLevel.DEBUG)) {
      Some(block)
    } else {
      None
    }
  }

  /**
   * Affiche un message de niveau INFO sans préfixe
   * @param msg Message à afficher
   * @param configuration Configuration de l'application
   */
  def infoPrintln(msg: => String)(implicit configuration: AppConfiguration): Unit = {
    if (shouldLog(LogLevel.INFO)) {
      println(msg)
    }
  }

  /**
   * Exécute un bloc de code uniquement si le niveau INFO est actif
   * @param block Bloc de code à exécuter
   * @param configuration Configuration de l'application
   */
  def whenInfo[T](block: => T)(implicit configuration: AppConfiguration): Option[T] = {
    if (shouldLog(LogLevel.INFO)) {
      Some(block)
    } else {
      None
    }
  }
}
