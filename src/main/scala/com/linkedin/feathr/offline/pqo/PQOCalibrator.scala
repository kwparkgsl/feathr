package com.linkedin.feathr.offline.pqo

import com.linkedin.feathr.offline.logical.FeatureGroups
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable


object PQOCalibrator {

  private val featureSourcePathSet: mutable.Set[String] = mutable.Set[String]()

  def calibrateJobCost(sparkSession: SparkSession,
                       featureGroups: FeatureGroups,
                       aggWindowProvenance: String): Boolean ={
    var shouldOptimize = false
    var avgCardinality: Float = 0
    var sumCardinality: Float = 0

    var aggWindowConf: String = ""
    var aggWindowConfInMin: Long = 0

    var aggWindowProvenanceInMin: Long = 0

    println("== Estimating Cost of the Job ==")

    val allAnchoredFeatures = featureGroups.allAnchoredFeatures

    hdfsFileReader(sparkSession, "/feathr_config/anchored_feature_define_pqo.conf").split("\n").foreach {
      confString =>
        if(confString.contains("window:")) {
          aggWindowConf = confString.replace(" ", "").split(":").toList(1)
        }
    }

    if(aggWindowProvenance.endsWith("d")) {
      aggWindowProvenanceInMin = aggWindowConf.replace("d", "").toLong
    } else {
      throw new RuntimeException("Unsupported Time Unit for Aggregation Time Window")
    }

    if (aggWindowConf.endsWith("d")) {
      aggWindowConfInMin = aggWindowConf.replace("d", "").toLong
    } else {
      throw new RuntimeException("Unsupported Time Unit for Aggregation Time Window")
    }

    /*
    val maxDurationPerSource = anchors
      .map(SlidingWindowFeatureUtils.getMaxWindowDurationInAnchor(_, windowAggFeatureNames))
      .max
    */
    allAnchoredFeatures.values.foreach(featureParam => featureSourcePathSet.add(featureParam.source.path))

    for(sourcePath <- featureSourcePathSet) {
      val sourceDF = sparkSession.read.format("csv").option("header", "true").load(sourcePath)
      val sourceRowNum = sourceDF.count()
      val exprs = sourceDF.columns.map(_ -> "approx_count_distinct").toMap
      val sourceDistinctDF = sourceDF.agg(exprs)
      sourceDF.columns.toList.foreach {
        columnName =>
          val columnDistinctName = "approx_count_distinct(" + columnName + ")"
          val distinctNum = sourceDistinctDF.select(sourceDistinctDF(columnDistinctName)).head().getLong(0)
          sumCardinality += distinctNum.toFloat / sourceRowNum
      }

      val differenceRate =  (aggWindowProvenanceInMin - aggWindowConfInMin).abs / aggWindowConfInMin.toFloat
      println(s"Different Rate: ${differenceRate}")

      println(s"Sum Cardinality: ${sumCardinality}")
      avgCardinality = sumCardinality / sourceDF.columns.length
      if(avgCardinality >= 0.5 && differenceRate <= 0.5) {
        shouldOptimize = true
      } else {
        shouldOptimize = false
      }
    }

    println(s"Should We Optimize the Job: ${shouldOptimize}")
    shouldOptimize
  }

  def hdfsFileReader(ss: SparkSession, path: String): String = {
    ss.sparkContext.textFile(path).collect.mkString("\n")
  }
}
