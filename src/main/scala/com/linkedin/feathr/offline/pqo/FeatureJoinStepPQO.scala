package com.linkedin.feathr.offline.pqo

import com.linkedin.feathr.common.ErasedEntityTaggedFeature
import com.linkedin.feathr.offline.join.workflow.{DataFrameJoinStepInput, DataFrameJoinStepOutput}

private[offline] trait FeatureJoinStepPQO[T <: DataFrameJoinStepInput, U <: DataFrameJoinStepOutput] {
  def joinFeatures(features: Seq[ErasedEntityTaggedFeature], input: T)(implicit ctx: JoinExecutionContextPQO): U
}
