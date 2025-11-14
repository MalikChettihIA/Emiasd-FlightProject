package com.flightdelay.utils

import com.flightdelay.config.AppConfiguration
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import com.flightdelay.utils.DebugUtils._

/**
 * Classe utilitaire pour écrire des DataFrames dans des fichiers CSV
 * Compatible Scala 2.12.18
 */
object CsvWriter {

  /**
   * Configuration pour l'écriture CSV
   */
  case class CsvConfig(
                        header: Boolean = true,
                        delimiter: String = ",",
                        quote: String = "\"",
                        escape: String = "\\",
                        nullValue: String = "",
                        dateFormat: String = "yyyy-MM-dd",
                        timestampFormat: String = "yyyy-MM-dd HH:mm:ss",
                        compression: String = "none", // none, gzip, bzip2, lz4, snappy
                        mode: SaveMode = SaveMode.Overwrite
                      )

  /**
   * Écrit un DataFrame dans un fichier CSV avec configuration par défaut
   *
   * @param df DataFrame à écrire
   * @param outputPath Chemin de sortie
   * @param spark SparkSession implicite
   */
  def write(df: DataFrame, outputPath: String)(implicit spark: SparkSession, configuration: AppConfiguration): Unit = {
    info("")
    info("- Writing Csv "+outputPath)
    write(df, outputPath, CsvConfig())
  }

  /**
   * Écrit un DataFrame dans un fichier CSV avec configuration personnalisée
   *
   * @param df DataFrame à écrire
   * @param outputPath Chemin de sortie
   * @param config Configuration CSV
   * @param spark SparkSession implicite
   */
  def write(df: DataFrame, outputPath: String, config: CsvConfig)(implicit spark: SparkSession, configuration: AppConfiguration): Unit = {
    df.write
      .mode(config.mode)
      .option("header", config.header.toString)
      .option("delimiter", config.delimiter)
      .option("quote", config.quote)
      .option("escape", config.escape)
      .option("nullValue", config.nullValue)
      .option("dateFormat", config.dateFormat)
      .option("timestampFormat", config.timestampFormat)
      .option("compression", config.compression)
      .csv(outputPath)

    println(s"- DataFrame written successfully to: $outputPath")
  }

  /**
   * Écrit un DataFrame dans UN SEUL fichier CSV
   *
   * @param df DataFrame à écrire
   * @param outputPath Chemin de sortie
   * @param config Configuration CSV
   * @param spark SparkSession implicite
   */
  def writeSingleFile(
                       df: DataFrame,
                       outputPath: String,
                       config: CsvConfig = CsvConfig()
                     )(implicit spark: SparkSession, configuration: AppConfiguration): Unit = {

    df.coalesce(1)
      .write
      .mode(config.mode)
      .option("header", config.header.toString)
      .option("delimiter", config.delimiter)
      .option("quote", config.quote)
      .option("escape", config.escape)
      .option("nullValue", config.nullValue)
      .option("dateFormat", config.dateFormat)
      .option("timestampFormat", config.timestampFormat)
      .option("compression", config.compression)
      .csv(outputPath)

    println(s"- DataFrame written to single file: $outputPath")
  }

  /**
   * Écrit un DataFrame dans UN SEUL fichier CSV avec un nom spécifique
   *
   * @param df DataFrame à écrire
   * @param outputPath Chemin du répertoire de sortie
   * @param fileName Nom du fichier final (sans extension .csv)
   * @param config Configuration CSV
   * @param spark SparkSession implicite
   */
  def writeSingleFileWithName(
                               df: DataFrame,
                               outputPath: String,
                               fileName: String,
                               config: CsvConfig = CsvConfig()
                             )(implicit spark: SparkSession, configuration: AppConfiguration): Unit = {

    val tempPath = s"$outputPath/temp_${System.currentTimeMillis()}"

    // Écrire dans un répertoire temporaire
    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", config.header.toString)
      .option("delimiter", config.delimiter)
      .option("quote", config.quote)
      .option("escape", config.escape)
      .option("nullValue", config.nullValue)
      .option("dateFormat", config.dateFormat)
      .option("timestampFormat", config.timestampFormat)
      .option("compression", config.compression)
      .csv(tempPath)

    // Renommer le fichier
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val srcPath = new Path(tempPath)
    val destDir = new Path(outputPath)

    // Créer le répertoire de destination si nécessaire
    if (!fs.exists(destDir)) {
      fs.mkdirs(destDir)
    }

    // Trouver le fichier part-*
    val partFiles = fs.globStatus(new Path(s"$tempPath/part-*"))
    if (partFiles.length > 0) {
      val partFile = partFiles(0).getPath
      val extension = if (config.compression == "none") ".csv" else s".csv.${config.compression}"
      val destPath = new Path(s"$outputPath/$fileName$extension")

      // Supprimer le fichier de destination s'il existe
      if (fs.exists(destPath)) {
        fs.delete(destPath, false)
      }

      // Renommer
      fs.rename(partFile, destPath)
      println(s"- File written successfully: $destPath")
    }

    // Nettoyer le répertoire temporaire
    fs.delete(srcPath, true)
  }

  /**
   * Écrit plusieurs DataFrames dans des fichiers CSV
   *
   * @param dataFrames Map de (nom -> DataFrame)
   * @param outputDir Répertoire de sortie
   * @param config Configuration CSV
   * @param spark SparkSession implicite
   */
  def writeMultiple(
                     dataFrames: Map[String, DataFrame],
                     outputDir: String,
                     config: CsvConfig = CsvConfig()
                   )(implicit spark: SparkSession, configuration: AppConfiguration): Unit = {

    dataFrames.foreach { case (name, df) =>
      val path = s"$outputDir/$name"
      write(df, path, config)
    }

    println(s"- ${dataFrames.size} DataFrames written to: $outputDir")
  }

  /**
   * Écrit un DataFrame partitionné par colonne(s)
   *
   * @param df DataFrame à écrire
   * @param outputPath Chemin de sortie
   * @param partitionCols Colonnes de partitionnement
   * @param config Configuration CSV
   * @param spark SparkSession implicite
   */
  def writePartitioned(
                        df: DataFrame,
                        outputPath: String,
                        partitionCols: Seq[String],
                        config: CsvConfig = CsvConfig()
                      )(implicit spark: SparkSession, configuration: AppConfiguration): Unit = {

    df.write
      .mode(config.mode)
      .partitionBy(partitionCols: _*)
      .option("header", config.header.toString)
      .option("delimiter", config.delimiter)
      .option("quote", config.quote)
      .option("escape", config.escape)
      .option("nullValue", config.nullValue)
      .option("dateFormat", config.dateFormat)
      .option("timestampFormat", config.timestampFormat)
      .option("compression", config.compression)
      .csv(outputPath)

    println(s"- Partitioned DataFrame written to: $outputPath")
    println(s"  Partition columns: ${partitionCols.mkString(", ")}")
  }
}