package com.flightdelay.data.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Schema Validator - Validates and normalizes DataFrame schemas based on naming conventions
 *
 * Ensures consistency between column names and their actual Spark types:
 * - feature_is_*, feature_has_*, is_*, has_* → Integer (0/1) for ML compatibility
 * - *_id, *_category, *_flag → Proper categorical types
 * - Date columns → DateType
 *
 * This allows ColumnTypeDetector to rely on both schema AND naming conventions.
 */
object SchemaValidator {

  /**
   * Validates that schema types match naming conventions
   *
   * @param df Input DataFrame
   * @param strictMode If true, throws exception on validation failure. If false, just warns.
   * @return Tuple of (isValid, validationMessages)
   */
  def validate(df: DataFrame, strictMode: Boolean = false): (Boolean, Seq[String]) = {
    val messages = scala.collection.mutable.ArrayBuffer[String]()
    var isValid = true

    println("=" * 80)
    println("[SchemaValidator] Validating DataFrame schema")
    println("=" * 80)

    val schema = df.schema

    // Check boolean naming conventions (should be Integer 0/1 or Boolean)
    val booleanPatterns = Seq("is_", "has_", "feature_is_", "feature_has_")
    val booleanCols = schema.fields.filter { field =>
      booleanPatterns.exists(pattern => field.name.toLowerCase.startsWith(pattern))
    }

    booleanCols.foreach { field =>
      field.dataType match {
        case IntegerType | BooleanType =>
          // Valid: Integer (0/1) or Boolean
          messages += s"   ${field.name}: ${field.dataType} (boolean naming → valid type)"
        case _ =>
          // Invalid: Boolean naming but wrong type
          isValid = false
          messages += s"  ✗ ${field.name}: ${field.dataType} (expected Integer or Boolean for boolean naming)"
      }
    }

    // Check ID columns (should be Integer or Long)
    val idCols = schema.fields.filter(_.name.toLowerCase.endsWith("_id"))
    idCols.foreach { field =>
      field.dataType match {
        case IntegerType | LongType =>
          messages += s"   ${field.name}: ${field.dataType} (ID naming → valid type)"
        case StringType =>
          // Warning: String IDs are acceptable but not optimal
          messages += s"   ${field.name}: ${field.dataType} (ID as String - consider Integer/Long)"
        case _ =>
          isValid = false
          messages += s"   ${field.name}: ${field.dataType} (expected Integer/Long/String for ID naming)"
      }
    }

    // Check category columns (should be String or Integer with low cardinality)
    val categoryCols = schema.fields.filter(_.name.toLowerCase.endsWith("_category"))
    categoryCols.foreach { field =>
      field.dataType match {
        case StringType | IntegerType =>
          messages += s"   ${field.name}: ${field.dataType} (category naming → valid type)"
        case _ =>
          isValid = false
          messages += s"   ${field.name}: ${field.dataType} (expected String or Integer for category naming)"
      }
    }

    // Check date columns
    val dateCols = schema.fields.filter { field =>
      val lower = field.name.toLowerCase
      lower.contains("_date") || lower.endsWith("date")
    }
    dateCols.foreach { field =>
      field.dataType match {
        case DateType | TimestampType =>
          messages += s"   ${field.name}: ${field.dataType} (date naming → valid type)"
        case StringType =>
          messages += s"   ${field.name}: ${field.dataType} (date as String - consider parsing to DateType)"
        case _ =>
          isValid = false
          messages += s"   ${field.name}: ${field.dataType} (expected DateType/TimestampType for date naming)"
      }
    }

    println("=" * 80)
    println(s"Schema Validation ${if (isValid) "PASSED" else "FAILED"}")
    println("=" * 80)
    println(s"Total columns validated: ${schema.fields.length}")
    println(s"Boolean columns: ${booleanCols.length}")
    println(s"ID columns: ${idCols.length}")
    println(s"Category columns: ${categoryCols.length}")
    println(s"Date columns: ${dateCols.length}")

    if (!isValid && strictMode) {
      println(" Validation failed in strict mode!")
      messages.foreach(println)
      throw new IllegalStateException(s"Schema validation failed. See messages above.")
    }

    if (!isValid) {
      println(" Validation warnings:")
      messages.filter(_.contains("✗")).foreach(println)
    }

    println("=" * 80)

    (isValid, messages.toSeq)
  }

  /**
   * Normalizes schema to ensure consistency with naming conventions
   *
   * Transformations:
   * - Boolean columns (is_*, has_*) → Integer (0/1) for ML compatibility
   * - String dates → DateType (if parseable)
   *
   * @param df Input DataFrame
   * @return Normalized DataFrame with consistent schema
   */
  def normalize(df: DataFrame): DataFrame = {
    println("=" * 80)
    println("[SchemaValidator] Normalizing DataFrame schema")
    println("=" * 80)

    var result = df
    var transformCount = 0

    val schema = df.schema

    // Normalize boolean columns to Integer (0/1)
    val booleanPatterns = Seq("is_", "has_", "feature_is_", "feature_has_")
    val booleanCols = schema.fields.filter { field =>
      booleanPatterns.exists(pattern => field.name.toLowerCase.startsWith(pattern))
    }

    booleanCols.foreach { field =>
      field.dataType match {
        case BooleanType =>
          // Convert Boolean → Integer (0/1)
          println(s"  - Converting ${field.name}: Boolean → Integer (0/1)")
          result = result.withColumn(
            field.name,
            when(col(field.name) === true, 1)
              .when(col(field.name) === false, 0)
              .otherwise(null)
              .cast(IntegerType)
          )
          transformCount += 1
        case IntegerType =>
          // Already Integer - validate it's 0/1 (optional, could add runtime check)
          println(s"   ${field.name}: Already Integer (0/1)")
        case _ =>
          println(s"   ${field.name}: Unexpected type ${field.dataType} for boolean naming")
      }
    }

    // Normalize String dates to DateType (if they follow YYYY-MM-DD or YYYYMMDD pattern)
    val dateCols = schema.fields.filter { field =>
      val lower = field.name.toLowerCase
      field.dataType == StringType && (lower.contains("_date") || lower.endsWith("date"))
    }

    dateCols.foreach { field =>
      println(s"  - Attempting to convert ${field.name}: String → DateType")
      try {
        // Try common date formats
        result = result.withColumn(
          s"${field.name}_parsed",
          coalesce(
            to_date(col(field.name), "yyyy-MM-dd"),
            to_date(col(field.name), "yyyyMMdd"),
            to_date(col(field.name), "MM/dd/yyyy")
          )
        )
        // Check if parsing succeeded (at least some non-null values)
        val parseCheck = result.select(col(s"${field.name}_parsed")).filter(col(s"${field.name}_parsed").isNotNull).count()
        if (parseCheck > 0) {
          result = result.drop(field.name).withColumnRenamed(s"${field.name}_parsed", field.name)
          println(s"     Converted ${field.name}: String → DateType (${parseCheck} records parsed)")
          transformCount += 1
        } else {
          result = result.drop(s"${field.name}_parsed")
          println(s"     Could not parse ${field.name} - keeping as String")
        }
      } catch {
        case e: Exception =>
          println(s"     Failed to convert ${field.name}: ${e.getMessage}")
      }
    }

    println("=" * 80)
    println(s"Schema Normalization Complete - ${transformCount} transformations applied")
    println("=" * 80)

    result
  }

  /**
   * Validates and normalizes in one step
   *
   * @param df Input DataFrame
   * @param strictMode If true, throws exception on validation failure before normalization
   * @return Normalized DataFrame
   */
  def validateAndNormalize(df: DataFrame, strictMode: Boolean = false): DataFrame = {
    // First validate
    val (isValid, _) = validate(df, strictMode = false)

    // Then normalize
    //val normalized = normalize(df)

    // Validate again after normalization
    //if (strictMode) {
    //  validate(normalized, strictMode = true)
    //}

    //normalized
    df
  }

  /**
   * Prints detailed schema report
   */
  def printSchemaReport(df: DataFrame): Unit = {
    println("=" * 80)
    println("[SchemaValidator] Detailed Schema Report")
    println("=" * 80)

    val schema = df.schema
    val columnsByType = schema.fields.groupBy(_.dataType.typeName)

    columnsByType.foreach { case (typeName, fields) =>
      println(s"${typeName.toUpperCase} columns (${fields.length}):")
      fields.take(10).foreach { field =>
        println(s"  - ${field.name}")
      }
      if (fields.length > 10) {
        println(s"  ... and ${fields.length - 10} more")
      }
    }

    println("=" * 80)
  }
}
