package com.linkedin.feathr.offline.pqo

import com.linkedin.feathr.offline.logical.FeatureGroups
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.sketch.BloomFilter

private[offline] sealed trait ExecutionContextPQO

private[offline] case class JoinExecutionContextPQO(sparkSession: SparkSession,
                                                    logicalPlanPQO: MultiStageJoinPlanPQO,
                                                    featureGroups: FeatureGroups,
                                                    bloomFilters: Option[Map[Seq[Int], BloomFilter]] = None,
                                                    frequentItemEstimatedDFMap: Option[Map[Seq[Int], DataFrame]] = None) extends ExecutionContextPQO
