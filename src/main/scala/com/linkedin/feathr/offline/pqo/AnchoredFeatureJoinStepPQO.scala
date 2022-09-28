package com.linkedin.feathr.offline.pqo

import com.linkedin.feathr.common.exception.{ErrorLabel, FeathrFeatureJoinException}
import com.linkedin.feathr.common.{ErasedEntityTaggedFeature, FeatureTypeConfig}
import com.linkedin.feathr.offline.FeatureDataFrame
import com.linkedin.feathr.offline.anchored.feature.FeatureAnchorWithSource
import com.linkedin.feathr.offline.client.DataFrameColName
import com.linkedin.feathr.offline.job.FeatureTransformation.{FEATURE_NAME_PREFIX, pruneAndRenameColumnWithTags, transformFeatures}
import com.linkedin.feathr.offline.job.KeyedTransformedResult
import com.linkedin.feathr.offline.join.algorithms.{JoinKeyColumnsAppender, SaltedJoinKeyColumnAppender, SaltedSparkJoin, SparkJoinWithJoinCondition}
import com.linkedin.feathr.offline.join.util.FrequentItemEstimatorFactory
import com.linkedin.feathr.offline.join.workflow.{AnchorJoinStepInput, DataFrameJoinStepOutput, FeatureDataFrameOutput, FeatureJoinStep}
import com.linkedin.feathr.offline.source.accessor.DataSourceAccessor
import com.linkedin.feathr.offline.transformation.DataFrameDefaultValueSubstituter.substituteDefaults
import com.linkedin.feathr.offline.util.FeathrUtils
import com.linkedin.feathr.offline.join.algorithms._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

private[offline] class AnchoredFeatureJoinStepPQO(leftJoinColumnExtractor: JoinKeyColumnsAppender,
                                                  rightJoinColumnExtractor: JoinKeyColumnsAppender,
                                                  joiner: SparkJoinWithJoinCondition) extends FeatureJoinStepPQO[AnchorJoinStepInput, DataFrameJoinStepOutput] {

  override def joinFeatures(features: Seq[ErasedEntityTaggedFeature], input: AnchorJoinStepInput)
                           (implicit ctxPQO: JoinExecutionContextPQO): FeatureDataFrameOutput = {

    val AnchorJoinStepInput(observationDF, anchorDFMap) = input
    val allAnchoredFeatures: Map[String, FeatureAnchorWithSource] = ctxPQO.featureGroups.allAnchoredFeatures
    val joinStages = ctxPQO.logicalPlanPQO.joinStages
    val enableCheckPoint = FeathrUtils.getFeathrJobParam(ctxPQO.sparkSession, FeathrUtils.ENABLE_CHECKPOINT).toBoolean

    val joinOutput = joinStages
      .foldLeft(FeatureDataFrame(observationDF, Map.empty[String, FeatureTypeConfig]))((accFeatureDataFrame, joinStage) => {
        val (keyTags: Seq[Int], featureNames: Seq[String]) = joinStage
        val FeatureDataFrame(contextDF, inferredFeatureTypeMap) = accFeatureDataFrame
        // map feature name to its transformed dataframe and the join key of the dataframe
        val groupedFeatureToDFAndJoinKeys: Map[Seq[String], Seq[KeyedTransformedResult]] = extractFeatureDataAndJoinKeys(keyTags, featureNames, allAnchoredFeatures, anchorDFMap)
        val tagsInfo = keyTags.map(ctxPQO.logicalPlanPQO.keyTagIntsToStrings).toList
        // use expr to support transformed keys for observation
        // Note that dataframe.join() can handle string type join with numeric type correctly, so we don't need to cast all
        // key types to string explicitly as what we did for the RDD version
        val (leftJoinColumns, contextDFWithJoinKey) = if (isSaltedJoinRequiredForKeys(keyTags)) {
          SaltedJoinKeyColumnAppender.appendJoinKeyColunmns(tagsInfo, contextDF)
        } else {
          leftJoinColumnExtractor.appendJoinKeyColunmns(tagsInfo, contextDF)
        }

        // Compute default values and feature types, for the features joined in the stage
        val anchoredDFThisStage = anchorDFMap.filterKeys(featureNames.filter(allAnchoredFeatures.contains).map(allAnchoredFeatures).toSet)
        val defaultValuesThisStage = FeatureAnchorWithSource.getDefaultValues(anchoredDFThisStage.keys.toSeq)
        val featureTypesThisStage = FeatureAnchorWithSource.getFeatureTypes(anchoredDFThisStage.keys.toSeq)
        // join features within the same stage, these features might be in different dataframes, so we join the dataframes in sequential order
        val (withBasicAnchorFeaturesContextDF, inferredTypes) =
          groupedFeatureToDFAndJoinKeys
            .foldLeft((contextDFWithJoinKey, Map.empty[String, FeatureTypeConfig]))((baseDFAndInferredTypes, featureToDFAndJoinKey) => {
              val (baseDF, prevInferredTypes) = baseDFAndInferredTypes
              val df = joinFeaturesOnSingleDF(keyTags, leftJoinColumns, baseDF, featureToDFAndJoinKey)

              // substitute default values for the feature joined
              val withDefaultDF =
                substituteDefaults(
                  df,
                  featureToDFAndJoinKey._1,
                  defaultValuesThisStage,
                  featureTypesThisStage,
                  ctxPQO.sparkSession,
                  (s: String) => s"${FEATURE_NAME_PREFIX}$s")
              // prune columns from feature data and rename key columns
              val renamedDF = pruneAndRenameColumns(tagsInfo, withDefaultDF, featureToDFAndJoinKey, baseDF.columns)
              (renamedDF, prevInferredTypes ++ featureToDFAndJoinKey._2.head.transformedResult.inferredFeatureTypes)
            })
        // remove left join key columns
        val withNoLeftJoinKeyDF = withBasicAnchorFeaturesContextDF.drop(leftJoinColumns: _*)
        // println("contextDF after dropping left join key columns:")
        // withNoLeftJoinKeyDF.show(false)

        val checkpointDF = if (enableCheckPoint) {
          // checkpoint complicated dataframe for each stage to avoid Spark failure
          withNoLeftJoinKeyDF.checkpoint(true)
        } else {
          withNoLeftJoinKeyDF
        }
        FeatureDataFrame(checkpointDF, inferredFeatureTypeMap ++ inferredTypes)
      })
    FeatureDataFrameOutput(joinOutput)
  }

  def extractFeatureDataAndJoinKeys(keyTags: Seq[Int],
                                    featureNames: Seq[String],
                                    allAnchoredFeatures: Map[String, FeatureAnchorWithSource],
                                    anchorDFMap: Map[FeatureAnchorWithSource, DataSourceAccessor])
                                   (implicit ctx: JoinExecutionContextPQO): Map[Seq[String], Seq[KeyedTransformedResult]] = {
    val bloomFilter = ctx.bloomFilters.map(filters => filters(keyTags))
    val (anchoredFeatureNamesThisStage, _) = featureNames.partition(allAnchoredFeatures.contains)
    val anchoredFeaturesThisStage = featureNames.filter(allAnchoredFeatures.contains).map(allAnchoredFeatures).distinct
    val anchoredDFThisStage = anchorDFMap.filterKeys(anchoredFeaturesThisStage.toSet)
    // map feature name to its transformed dataframe and the join key of the dataframe
    val featureToDFAndJoinKeys = transformFeatures(anchoredDFThisStage, anchoredFeatureNamesThisStage, bloomFilter)
    featureToDFAndJoinKeys
      .groupBy(_._2.transformedResult.df) // group by dataframe, join one at a time
      .map(grouped => (grouped._2.keys.toSeq, grouped._2.values.toSeq)) // extract the feature names and their (dataframe,join keys) pairs
  }

  def joinFeaturesOnSingleDF(keyTags: Seq[Int],
                             leftJoinColumns: Seq[String],
                             contextDF: DataFrame,
                             featureToDFAndJoinKey: (Seq[String], Seq[KeyedTransformedResult]))
                            (implicit ctx: JoinExecutionContextPQO): DataFrame = {
    // since we group by dataframe already, all the join keys in this featureToDFAndJoinKey are the same, just take the first one
    if (featureToDFAndJoinKey._2.map(_.joinKey).toList.distinct.size != 1) {
      throw new FeathrFeatureJoinException(ErrorLabel.FEATHR_ERROR,
        s"In AnchoredFeatureJoinStep.joinFeaturesOnSingleDF, all features should have same join key size, but found ${featureToDFAndJoinKey._2.map(_.joinKey).toList}")
    }
    val rawRightJoinColumnSize = featureToDFAndJoinKey._2.head.joinKey.size
    val rawRightJoinKeys = featureToDFAndJoinKey._2.head.joinKey
    val transformedResult = featureToDFAndJoinKey._2.head.transformedResult
    val featureDF = transformedResult.df
    if (rawRightJoinColumnSize == 0) {
      // when rightDF is empty, MVEL default source key extractor might return 0 key columns
      // in such cases, we just append the right table schema to the left table,
      // so that default value can be applied later
      featureDF.columns
        .zip(featureDF.schema.fields)
        .foldRight(contextDF)((nameAndfield, inputDF) => {
          inputDF.withColumn(nameAndfield._1, lit(null).cast(nameAndfield._2.dataType))
        })
    } else {
      if (isSaltedJoinRequiredForKeys(keyTags)) {
        val (rightJoinColumns, rightDF) = SaltedJoinKeyColumnAppender.appendJoinKeyColunmns(rawRightJoinKeys, featureDF)
        println(s"rightJoinColumns= [${rightJoinColumns.mkString(", ")}] features= [${featureToDFAndJoinKey._1.mkString(", ")}]")
        val saltedJoinFrequentItemDF = ctx.frequentItemEstimatedDFMap.get(keyTags)
        val saltedJoiner = new SaltedSparkJoin(ctx.sparkSession, FrequentItemEstimatorFactory.createFromCache(saltedJoinFrequentItemDF))
        saltedJoiner.join(leftJoinColumns, contextDF, rightJoinColumns, rightDF, JoinType.left_outer)
      } else {
        val (rightJoinColumns, rightDF) = rightJoinColumnExtractor.appendJoinKeyColunmns(rawRightJoinKeys, featureDF)
        println(s"rightJoinColumns= [${rightJoinColumns.mkString(", ")}] features= [${featureToDFAndJoinKey._1.mkString(", ")}]")
        joiner.join(leftJoinColumns, contextDF, rightJoinColumns, rightDF, JoinType.left_outer)
      }

    }
  }

  def pruneAndRenameColumns(tagsInfo: Seq[String],
                            contextDF: DataFrame,
                            featureToDFAndJoinKey: (Seq[String], Seq[KeyedTransformedResult]),
                            columnsToKeep: Seq[String])(implicit ctx: JoinExecutionContextPQO): DataFrame = {
    val rightJoinKeysColumnsToDrop = featureToDFAndJoinKey._2.flatMap(_.joinKey)
    val featuresToRename = featureToDFAndJoinKey._1 map (refStr => DataFrameColName.getEncodedFeatureRefStrForColName(refStr))
    println(s"featuresToRename = $featuresToRename")

    val featureDF = featureToDFAndJoinKey._2.head.transformedResult.df
    val renamedDF = pruneAndRenameColumnWithTags(contextDF, columnsToKeep, featuresToRename, featureDF.columns, tagsInfo.toList)

    // right join key column are duplicated
    renamedDF.drop(rightJoinKeysColumnsToDrop: _*)
  }

  /**
   * Helper method that checks if input key tags have frequent items and requires salting.
   */
  private def isSaltedJoinRequiredForKeys(keyTags: Seq[Int])(implicit ctx: JoinExecutionContextPQO) =
    ctx.frequentItemEstimatedDFMap.isDefined && ctx.frequentItemEstimatedDFMap.get.contains(keyTags)
}

private[offline] object AnchoredFeatureJoinStepPQO {
  def apply(leftJoinColumnExtractor: JoinKeyColumnsAppender,
            rightJoinColumnExtractor: JoinKeyColumnsAppender,
            joiner: SparkJoinWithJoinCondition): AnchoredFeatureJoinStepPQO =
    new AnchoredFeatureJoinStepPQO(leftJoinColumnExtractor, rightJoinColumnExtractor, joiner)
}
