package com.linkedin.feathr.offline.pqo

import com.linkedin.feathr.common.JoiningFeatureParams
import com.linkedin.feathr.offline.logical.FeatureGroups

private[offline] trait LogicalPlannerPQO[T <: LogicalPlanPQO] {
  def getLogicalPlanPQO(featureGroups: FeatureGroups, keyTaggedFeatures: Seq[JoiningFeatureParams]): T
}
