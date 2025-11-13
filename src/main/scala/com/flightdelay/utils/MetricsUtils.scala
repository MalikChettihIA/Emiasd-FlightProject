package com.flightdelay.utils

import org.apache.spark.sql.SparkSession

object MetricsUtils {

  def withUiLabels[T](
                       groupId: String,
                       desc: String,
                       tags: String = ""
                     )(body: => T)(implicit spark: SparkSession): T = {

    val sc = spark.sparkContext
    sc.setJobGroup(groupId, desc, interruptOnCancel = true)
    sc.setLocalProperty("spark.job.description", desc)
    sc.setLocalProperty("spark.jobGroup.id", groupId)
    sc.setLocalProperty("spark.job.tags", tags)

    val startTime = System.currentTimeMillis()

    try {
      println(s"${desc} → Starting job: $desc")
      val result = body
      val duration = (System.currentTimeMillis() - startTime) / 1000.0
      println(f"${desc} ✓ Completed in ${duration}%.2f s")
      result
    } finally {
      sc.clearJobGroup()
      sc.setLocalProperty("spark.job.description", null)
      sc.setLocalProperty("spark.jobGroup.id", null)
      sc.setLocalProperty("spark.job.tags", null)
    }
  }

}
