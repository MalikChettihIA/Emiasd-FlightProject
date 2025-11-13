package com.flightdelay.data.preprocessing

import com.flightdelay.config.AppConfiguration
import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.flightdelay.utils.DebugUtils._

/**
 * Classe abstraite définissant l'interface commune pour le preprocessing des données.
 *
 */
abstract class DataPreprocessor {

  /**
   * Méthode principale de preprocessing
   * @param spark Session Spark
   * @param configuration Configuration de l'application (peut être null si non utilisée)
   * @param rawData DataFrame contenant les données brutes
   * @return DataFrame préprocessé
   */
  def preprocess(rawData: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration = null): DataFrame

  /**
   * Supprime les lignes dupliquées basées sur des colonnes spécifiques
   * @param df DataFrame d'entrée
   * @param columns Colonnes à considérer pour les doublons (optionnel)
   * @return DataFrame sans doublons
   */
  protected def removeDuplicates(df: DataFrame, columns: Seq[String] = Seq.empty): DataFrame = {
    val result = if (columns.nonEmpty) {
      df.dropDuplicates(columns)
    } else {
      df.dropDuplicates()
    }

    result
  }

  /**
   * Supprime les lignes avec des valeurs nulles dans certaines colonnes
   * @param df DataFrame d'entrée
   * @param columns Colonnes à vérifier pour les valeurs nulles
   * @return DataFrame filtré
   */
  protected def removeNullValues(df: DataFrame, columns: Seq[String]): DataFrame = {

    val result = columns.foldLeft(df) { (acc, columnName) =>
      acc.filter(col(columnName).isNotNull)
    }

    result
  }


  /**
   * Convertit les types de données selon un mapping spécifié
   * @param df DataFrame d'entrée
   * @param typeMapping Map[colonne -> type de données]
   * @return DataFrame avec les types convertis
   */
  protected def convertDataTypes(df: DataFrame, typeMapping: Map[String, DataType])(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {
    debug(s"Conversion des types de données: ${typeMapping.keys.mkString(", ")}")

    typeMapping.foldLeft(df) { case (acc, (colName, dataType)) =>
      if (acc.columns.contains(colName)) {
        acc.withColumn(colName, col(colName).cast(dataType))
      } else {
        debug(s"Colonne '$colName' introuvable pour conversion de type")
        acc
      }
    }
  }

  /**
   * Ajoute des colonnes calculées basées sur des expressions
   * @param df DataFrame d'entrée
   * @param columnExpressions Map[nom_nouvelle_colonne -> expression]
   * @return DataFrame avec les nouvelles colonnes
   */
  protected def addCalculatedColumns(df: DataFrame, columnExpressions: Map[String, Column]): DataFrame = {

    columnExpressions.foldLeft(df) { case (acc, (colName, expression)) =>
      acc.withColumn(colName, expression)
    }
  }



}
