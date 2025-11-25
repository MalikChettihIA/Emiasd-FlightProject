package com.flightdelay.config

/**
 * Output configuration
 * @param basePath Base path for output (can be HDFS or local)
 * @param data Data output configuration
 * @param model Model output configuration
 * @param localPath Optional local path for copying HDFS artifacts (e.g., CephFS)
 *                  If specified, HDFS files will be copied here for visualization and MLFlow
 */
case class OutputConfig(
   basePath: String,
   data: FileConfig,
   model: FileConfig,
   localPath: Option[String] = None
)
