package com.flightdelay.utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.io.File
import scala.util.{Try, Success, Failure}

/**
 * Helper object to manage HDFS to local filesystem operations
 * Useful for copying metrics and CSV files from HDFS to local storage (e.g., CephFS)
 * for Python visualization scripts and MLFlow artifact logging
 */
object HDFSHelper {

  /**
   * Copy a directory from HDFS to local filesystem
   * Only performs copy if source is HDFS (starts with hdfs://)
   *
   * @param spark SparkSession for Hadoop configuration
   * @param hdfsPath Source path (can be HDFS or local)
   * @param localPath Destination path in local filesystem
   * @return Local path (either copied destination or original path if not HDFS)
   */
  def copyToLocalIfNeeded(
    spark: SparkSession,
    hdfsPath: String,
    localPath: String
  ): Try[String] = {

    // If not HDFS, return original path
    if (!isHDFSPath(hdfsPath)) {
      println(s"[HDFSHelper] Path is already local: $hdfsPath")
      return Success(hdfsPath)
    }

    // Perform HDFS → Local copy
    Try {
      val conf = spark.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(conf)

      val srcPath = new Path(hdfsPath)

      // Check if source exists
      if (!fs.exists(srcPath)) {
        throw new IllegalArgumentException(s"Source path does not exist in HDFS: $hdfsPath")
      }

      println(s"[HDFSHelper] Copying from HDFS to local...")
      println(s"[HDFSHelper]   Source: $hdfsPath")
      println(s"[HDFSHelper]   Destination: $localPath")

      // Create local directory
      val localDir = new File(localPath)
      if (!localDir.exists()) {
        localDir.mkdirs()
        println(s"[HDFSHelper]   Created local directory: $localPath")
      } else {
        // Clean existing directory
        deleteLocalRecursively(localDir)
        localDir.mkdirs()
        println(s"[HDFSHelper]   Cleaned existing directory: $localPath")
      }

      // Copy directory recursively
      copyDirectoryRecursive(fs, srcPath, localPath)

      fs.close()
      println(s"[HDFSHelper] ✓ Copy completed successfully")

      localPath
    }
  }

  /**
   * Copy a directory from HDFS to local recursively
   */
  private def copyDirectoryRecursive(fs: FileSystem, srcPath: Path, localDirPath: String): Unit = {
    val status = fs.listStatus(srcPath)

    status.foreach { fileStatus =>
      val fileName = fileStatus.getPath.getName

      // Skip hidden files and Hadoop metadata files
      if (!fileName.startsWith("_") && !fileName.startsWith(".")) {
        val srcFile = fileStatus.getPath
        val localFile = new File(localDirPath, fileName)

        if (fileStatus.isDirectory) {
          // Create local subdirectory and copy recursively
          localFile.mkdirs()
          copyDirectoryRecursive(fs, srcFile, localFile.getAbsolutePath)
        } else {
          // Copy file
          val localFilePath = new Path(localFile.getAbsolutePath)
          fs.copyToLocalFile(false, srcFile, localFilePath, true)
          println(s"[HDFSHelper]     Copied: $fileName")
        }
      }
    }
  }

  /**
   * Check if path is HDFS path
   */
  def isHDFSPath(path: String): Boolean = {
    path.startsWith("hdfs://")
  }

  /**
   * Delete local file or directory recursively
   */
  private def deleteLocalRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteLocalRecursively)
    }
    file.delete()
  }

  /**
   * Copy experiment metrics from HDFS to local for visualization and MLFlow
   * This is a smart copy that only executes if:
   * 1. localBasePath is provided (Some)
   * 2. hdfsPath is actually HDFS (starts with hdfs://)
   *
   * @param spark SparkSession
   * @param hdfsExperimentPath HDFS path for experiment output
   * @param localBasePath Optional local base path (e.g., CephFS)
   * @param experimentName Experiment name
   * @return Path to use for visualization (local if copied, HDFS if not)
   */
  def copyExperimentMetrics(
    spark: SparkSession,
    hdfsExperimentPath: String,
    localBasePath: Option[String],
    experimentName: String
  ): String = {

    localBasePath match {
      case Some(localBase) =>
        // localPath is configured, attempt copy
        val localExpPath = s"$localBase/$experimentName"

        copyToLocalIfNeeded(spark, hdfsExperimentPath, localExpPath) match {
          case Success(path) =>
            path
          case Failure(e) =>
            println(s"[HDFSHelper] ⚠ Warning: Copy failed: ${e.getMessage}")
            println(s"[HDFSHelper] Falling back to HDFS path: $hdfsExperimentPath")
            hdfsExperimentPath
        }

      case None =>
        // No localPath configured, use HDFS path directly
        println(s"[HDFSHelper] No localPath configured, using: $hdfsExperimentPath")
        hdfsExperimentPath
    }
  }
}
