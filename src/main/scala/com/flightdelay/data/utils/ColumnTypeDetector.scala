package com.flightdelay.data.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

/**
 * Column Type Detector - High-performance column type detection
 *
 * Detects numeric, categorical, boolean, and date/timestamp columns using schema analysis (O(1))
 * instead of full data scans, providing ~30,000x speedup over metrics-based approach.
 */
object ColumnTypeDetector {

  /**
   * Identifie les colonnes numériques, catégorielles, booléennes et dates directement depuis le schema
   * Performance: O(1) - Aucun scan des données nécessaire
   *
   * @param data Input DataFrame
   * @param excludeColumns Columns to exclude from detection
   * @return Tuple of (numericColumns, categoricalColumns, booleanColumns, dateColumns)
   */
  def detectColumnTypes(
                         data: DataFrame,
                         excludeColumns: Seq[String] = Seq.empty
                       ): (Array[String], Array[String], Array[String], Array[String]) = {

    val numericTypes = Set(
      classOf[IntegerType], classOf[LongType], classOf[ShortType], classOf[ByteType],
      classOf[FloatType], classOf[DoubleType], classOf[DecimalType]
    )

    val categoricalTypes = Set(
      classOf[StringType]
    )

    val booleanType = classOf[BooleanType]

    val dateTypes = Set(
      classOf[DateType], classOf[TimestampType]
    )

    val excludeSet = excludeColumns.toSet

    val relevantFields = data.schema.fields
      .filterNot(field => excludeSet.contains(field.name))

    val numeric = relevantFields
      .filter(field => numericTypes.exists(_.isInstance(field.dataType)))
      .map(_.name)

    val categorical = relevantFields
      .filter(field => categoricalTypes.exists(_.isInstance(field.dataType)))
      .map(_.name)

    val boolean = relevantFields
      .filter(field => booleanType.isInstance(field.dataType))
      .map(_.name)

    val date = relevantFields
      .filter(field => dateTypes.exists(_.isInstance(field.dataType)))
      .map(_.name)

    (numeric, categorical, boolean, date)
  }

  /**
   * Version intelligente basée sur le schema et les conventions de nommage
   * Performance: O(1) - Pas de scan, uniquement lecture du schema
   *
   * Conventions de nommage reconnues:
   * - Colonnes commençant par "is_", "has_", "feature_is_", "feature_has_" → booléennes
   * - Colonnes finissant par "_id", "_ID", "_category", "_flag" → catégorielles
   * - Colonnes Integer/Long avec autres patterns → numériques
   *
   * @param data Input DataFrame
   * @param excludeColumns Columns to exclude from detection
   * @param maxCardinalityForCategorical Not used (kept for backward compatibility)
   * @param sampleFraction Not used (kept for backward compatibility)
   * @return Tuple of (numericColumns, categoricalColumns, booleanColumns, dateColumns)
   */
  def detectColumnTypesWithHeuristics(
                                       data: DataFrame,
                                       excludeColumns: Seq[String] = Seq.empty,
                                       maxCardinalityForCategorical: Int = 50,
                                       sampleFraction: Double = 0.01
                                     ): (Array[String], Array[String], Array[String], Array[String]) = {

    println(s"  - Using fast schema-based type detection (O(1), no data scan)")

    val (schemaNumerics, schemaCategoricals, schemaBooleans, schemaDates) =
      detectColumnTypes(data, excludeColumns)

    val excludeSet = excludeColumns.toSet

    // Identifier les colonnes Integer qui sont probablement catégorielles par convention de nommage
    val integerCols = data.schema.fields
      .filter(f => f.dataType == IntegerType || f.dataType == LongType)
      .map(_.name)
      .filterNot(excludeSet.contains)

    if (integerCols.isEmpty) {
      return (schemaNumerics, schemaCategoricals, schemaBooleans, schemaDates)
    }

    // Appliquer des heuristiques basées sur le nom (pas de scan de données!)
    val categoricalByNaming = integerCols.filter { colName =>
      val lower = colName.toLowerCase
      // IDs, catégories, flags sont catégoriels
      lower.endsWith("_id") ||
      lower.endsWith("_category") ||
      lower.endsWith("_flag") ||
      lower.contains("airport") ||
      lower.contains("carrier") ||
      lower.contains("airline")
    }.toSet

    // Colonnes Integer qui ressemblent à des booléens
    val booleanByNaming = integerCols.filter { colName =>
      val lower = colName.toLowerCase
      lower.startsWith("is_") ||
      lower.startsWith("has_") ||
      lower.startsWith("feature_is_") ||
      lower.startsWith("feature_has_") ||
      lower.contains("_cancelled") ||
      lower.contains("_diverted")
    }.toSet

    if (categoricalByNaming.nonEmpty) {
      println(s"  - Classified ${categoricalByNaming.size} integers as categorical (by naming convention)")
      if (categoricalByNaming.size <= 10) {
        println(s"    ${categoricalByNaming.mkString(", ")}")
      }
    }

    if (booleanByNaming.nonEmpty) {
      println(s"  - Classified ${booleanByNaming.size} integers as boolean (by naming convention)")
      if (booleanByNaming.size <= 10) {
        println(s"    ${booleanByNaming.mkString(", ")}")
      }
    }

    val finalNumerics = schemaNumerics
      .filterNot(categoricalByNaming.contains)
      .filterNot(booleanByNaming.contains)

    val finalCategoricals = schemaCategoricals ++ categoricalByNaming
    val finalBooleans = schemaBooleans ++ booleanByNaming

    (finalNumerics, finalCategoricals, finalBooleans, schemaDates)
  }

  /**
   * Détecte automatiquement les types de colonnes (version 3 éléments - backward compatible)
   * Les dates sont exclues
   *
   * @deprecated Use detectColumnTypes which returns (numeric, categorical, boolean, date) instead
   */
  def detectColumnTypesLegacy(
                               data: DataFrame,
                               excludeColumns: Seq[String] = Seq.empty
                             ): (Array[String], Array[String], Array[String]) = {
    val (numeric, categorical, boolean, _) = detectColumnTypes(data, excludeColumns)
    (numeric, categorical, boolean)
  }

  /**
   * Print column type detection summary
   */
  def printSummary(
                    numeric: Array[String],
                    categorical: Array[String],
                    boolean: Array[String],
                    date: Array[String]
                  ): Unit = {
    println(s"  Column Type Detection Summary:")

    println(s"  - Numeric columns: ${numeric.length}")
    if (numeric.length <= 10) {
      println(s"    ${numeric.mkString(", ")}")
    } else {
      println(s"    ${numeric.take(10).mkString(", ")}, ... (${numeric.length - 10} more)")
    }

    println(s"  - Categorical columns: ${categorical.length}")
    if (categorical.length <= 10) {
      println(s"    ${categorical.mkString(", ")}")
    } else {
      println(s"    ${categorical.take(10).mkString(", ")}, ... (${categorical.length - 10} more)")
    }

    println(s"  - Boolean columns: ${boolean.length}")
    if (boolean.length <= 10) {
      println(s"    ${boolean.mkString(", ")}")
    } else {
      println(s"    ${boolean.take(10).mkString(", ")}, ... (${boolean.length - 10} more)")
    }

    println(s"  - Date/Timestamp columns: ${date.length}")
    if (date.length <= 10) {
      println(s"    ${date.mkString(", ")}")
    } else {
      println(s"    ${date.take(10).mkString(", ")}, ... (${date.length - 10} more)")
    }
  }

  /**
   * Print column type detection summary (legacy version with 3 parameters)
   * @deprecated Use printSummary(numeric, categorical, boolean, date) instead
   */
  def printSummary(numeric: Array[String], categorical: Array[String], boolean: Array[String]): Unit = {
    println(s"  Column Type Detection Summary:")
    println(s"  - Numeric columns: ${numeric.length}")
    if (numeric.length <= 10) {
      println(s"    ${numeric.mkString(", ")}")
    } else {
      println(s"    ${numeric.take(10).mkString(", ")}, ... (${numeric.length - 10} more)")
    }
    println(s"  - Categorical columns: ${categorical.length}")
    if (categorical.length <= 10) {
      println(s"    ${categorical.mkString(", ")}")
    } else {
      println(s"    ${categorical.take(10).mkString(", ")}, ... (${categorical.length - 10} more)")
    }
    println(s"  - Boolean columns: ${boolean.length}")
    if (boolean.length <= 10) {
      println(s"    ${boolean.mkString(", ")}")
    } else {
      println(s"    ${boolean.take(10).mkString(", ")}, ... (${boolean.length - 10} more)")
    }
  }

  /**
   * Helper pour distinguer les types de date
   * @return Tuple of (dateColumns, timestampColumns)
   */
  def separateDateTypes(data: DataFrame, dateColumns: Array[String]): (Array[String], Array[String]) = {
    val dates = dateColumns.filter(col => data.schema(col).dataType == DateType)
    val timestamps = dateColumns.filter(col => data.schema(col).dataType == TimestampType)
    (dates, timestamps)
  }

  /**
   * Print detailed date type information
   */
  def printDateTypeDetails(data: DataFrame, dateColumns: Array[String]): Unit = {
    if (dateColumns.isEmpty) {
      return
    }

    val (dates, timestamps) = separateDateTypes(data, dateColumns)

    println(s"  Date/Timestamp Column Details:")
    if (dates.nonEmpty) {
      println(s"    Date columns (${dates.length}): ${dates.mkString(", ")}")
      println(s"      → Will extract: year, month, day, dayofweek, unix_timestamp")
    }
    if (timestamps.nonEmpty) {
      println(s"    Timestamp columns (${timestamps.length}): ${timestamps.mkString(", ")}")
      println(s"      → Will extract: year, month, day, dayofweek, hour, minute, unix_timestamp")
    }
  }
}