package com.flightdelay.examples

import com.flightdelay.features.quality.ParquetMissingValuesValidator
import org.apache.spark.sql.SparkSession

/**
 * Exemple d'utilisation du validateur de parquets
 *
 * Usage:
 * sbt "runMain com.flightdelay.examples.ValidateParquetExample"
 */
object ValidateParquetExample {

  def main(args: Array[String]): Unit = {

    // Créer la session Spark
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("ValidateParquetExample")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      // Définir les chemins vers les parquets
      val experimentName = "Experience-local-D-60-7-7"
      val basePath = "/output"

      val trainPath = s"$basePath/$experimentName/data/join_exploded_train_prepared.parquet"
      val testPath = s"$basePath/$experimentName/data/join_exploded_test_prepared.parquet"

      println("\n🚀 Démarrage de la validation des parquets...\n")

      // Option 1: Générer un rapport complet (TRAIN + TEST)
      ParquetMissingValuesValidator.generateReport(trainPath, testPath)

      // Option 2: Valider seulement TRAIN
      // ParquetMissingValuesValidator.validateParquet(trainPath, "TRAIN")

      // Option 3: Valider seulement TEST
      // ParquetMissingValuesValidator.validateParquet(testPath, "TEST")

      println("\n✅ Validation terminée avec succès !\n")

    } catch {
      case e: Exception =>
        println(s"\n❌ Erreur lors de la validation: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
