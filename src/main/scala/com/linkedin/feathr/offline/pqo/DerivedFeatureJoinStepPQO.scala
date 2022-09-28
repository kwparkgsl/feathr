package com.linkedin.feathr.offline.pqo

import com.linkedin.feathr.common
import com.linkedin.feathr.common.FeatureTypeConfig
import com.linkedin.feathr.offline.FeatureDataFrame
import com.linkedin.feathr.offline.client.DataFrameColName
import com.linkedin.feathr.offline.derived.DerivedFeatureEvaluator
import com.linkedin.feathr.offline.join.workflow.{DataFrameJoinStepInput, FeatureDataFrameOutput}


private[offline] class DerivedFeatureJoinStepPQO(derivedFeatureEvaluator: DerivedFeatureEvaluator)
  extends FeatureJoinStepPQO[DataFrameJoinStepInput, FeatureDataFrameOutput] {

  override def joinFeatures(features: Seq[common.ErasedEntityTaggedFeature],
                            input: DataFrameJoinStepInput)
                           (implicit ctxPQO: JoinExecutionContextPQO): FeatureDataFrameOutput = {
    val allDerivedFeatures = ctxPQO.featureGroups.allDerivedFeatures
    val joinStages = ctxPQO.logicalPlanPQO.joinStages ++
      ctxPQO.logicalPlanPQO.convertErasedEntityTaggedToJoinStage(ctxPQO.logicalPlanPQO.postJoinDerivedFeatures)

    val inputDF = input.observation

    val resultFeatureDataFrame =
      joinStages.foldLeft(FeatureDataFrame(inputDF, Map.empty[String, FeatureTypeConfig]))((accFeatureDataFrame, joinStage) => {
        val (keyTags: Seq[Int], featureNames: Seq[String]) = joinStage
        val FeatureDataFrame(contextDF, inferredFeatureTypeMap) = accFeatureDataFrame
        val (derivedFeaturesThisStage, _) = featureNames.partition(allDerivedFeatures.contains)

        // calculate derived feature scheduled in this stage
        val derivations = derivedFeaturesThisStage.map(f => (allDerivedFeatures(f), f))
        derivations.foldLeft(FeatureDataFrame(contextDF, inferredFeatureTypeMap))((derivedAccDataFrame, derivedFeature) => {
          val FeatureDataFrame(baseDF, inferredTypes) = derivedAccDataFrame
          val (derived, featureName) = derivedFeature
          val tagsInfo = keyTags.map(ctxPQO.logicalPlanPQO.keyTagIntsToStrings)
          val featureColumnName = DataFrameColName.genFeatureColumnName(featureName, Some(tagsInfo))
          // some features might have been generated together with other derived features already, i.e.,
          // if the derived feature column exist in the contextDF, skip the calculation, this is due to
          // multiple derived features can be provided by same derivation function, i.e., advanced derivation function.
          if (!baseDF.columns.contains(featureColumnName)) {
            val FeatureDataFrame(withDerivedContextDF, newInferredTypes) =
              derivedFeatureEvaluator.evaluate(keyTags, ctxPQO.logicalPlanPQO.keyTagIntsToStrings, baseDF, derived)

            // println(s"Final output after joining non-SWA features:")
            // contextDF.show(false)
            FeatureDataFrame(withDerivedContextDF, inferredTypes ++ newInferredTypes)
          } else {
            FeatureDataFrame(baseDF, inferredTypes)
          }
        })
      })
    FeatureDataFrameOutput(resultFeatureDataFrame)
  }
}

object DerivedFeatureJoinStepPQO {
  def apply(derivedFeatureEvaluator: DerivedFeatureEvaluator): DerivedFeatureJoinStepPQO =
    new DerivedFeatureJoinStepPQO(derivedFeatureEvaluator: DerivedFeatureEvaluator)
}