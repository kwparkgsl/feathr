package com.linkedin.feathr.offline.pqo

import com.linkedin.feathr.common
import com.linkedin.feathr.offline.{ErasedEntityTaggedFeature, FeatureName, JoinStage, KeyTagIdTuple}

private[offline] sealed trait LogicalPlanPQO

private[offline] case class MultiStageJoinPlanPQO(windowAggFeatureStages: Seq[(KeyTagIdTuple, Seq[FeatureName])],
                                                  joinStages: Seq[(KeyTagIdTuple, Seq[FeatureName])],
                                                  postJoinDerivedFeatures: Seq[common.ErasedEntityTaggedFeature],
                                                  requiredWindowAggFeatures: Seq[common.ErasedEntityTaggedFeature],
                                                  requiredNonWindowAggFeatures: Seq[common.ErasedEntityTaggedFeature],
                                                  seqJoinFeatures: Seq[common.ErasedEntityTaggedFeature],
                                                  keyTagIntsToStrings: Seq[String],
                                                  allRequiredFeatures: Seq[common.ErasedEntityTaggedFeature],
                                                  allRequestedFeatures: Seq[common.ErasedEntityTaggedFeature]) extends LogicalPlanPQO {

  def convertErasedEntityTaggedToJoinStage(erasedEntityTaggedFeatures: Seq[common.ErasedEntityTaggedFeature]): Seq[JoinStage] = {
    erasedEntityTaggedFeatures.foldLeft(Seq.empty[JoinStage])(
      (acc, erasedTaggedFeature) => {
        val ErasedEntityTaggedFeature(keyTag, featureName) = erasedTaggedFeature
        acc :+ (keyTag, Seq(featureName))
      }
    )
  }
}

