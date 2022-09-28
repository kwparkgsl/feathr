package com.linkedin.feathr.offline.pqo

import com.linkedin.feathr.common.{Header, JoiningFeatureParams, TaggedFeatureName}
import com.linkedin.feathr.offline.{ErasedEntityTaggedFeature, FeatureDataFrame}
import com.linkedin.feathr.offline.client.DataFrameColName
import com.linkedin.feathr.offline.client.DataFrameColName.getFeatureAlias
import com.linkedin.feathr.offline.config.FeatureJoinConfig
import com.linkedin.feathr.offline.derived.DerivedFeatureEvaluator
import com.linkedin.feathr.offline.join.algorithms._
import com.linkedin.feathr.offline.join.util.{FrequentItemEstimatorFactory, FrequentItemEstimatorType}
import com.linkedin.feathr.offline.join.workflow.{AnchorJoinStepInput, BaseJoinStepInput, FeatureDataFrameOutput}
import com.linkedin.feathr.offline.logical.FeatureGroups
import com.linkedin.feathr.offline.source.accessor.DataPathHandler
import com.linkedin.feathr.offline.swa.SlidingWindowAggregationJoiner
import com.linkedin.feathr.offline.transformation.AnchorToDataSourceMapper
import com.linkedin.feathr.offline.util.FeathrUtils
import com.linkedin.feathr.offline.util.datetime.DateTimeInterval

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.sketch.BloomFilter

import scala.collection.JavaConverters._

private[offline] class DataFrameFeatureJoinerPQO(logicalPlanPQO: MultiStageJoinPlanPQO,
                                                 dataPathHandlerList: List[DataPathHandler]) extends Serializable {

  @transient lazy val anchorToDataSourceMapper = new AnchorToDataSourceMapper(dataPathHandlerList)
  private val windowAggFeatureStages = logicalPlanPQO.windowAggFeatureStages
  private val joinStages = logicalPlanPQO.joinStages
  private val postJoinDerivedFeatures = logicalPlanPQO.postJoinDerivedFeatures
  private val requiredWindowAggFeatures = logicalPlanPQO.requiredWindowAggFeatures
  private val requiredNonWindowAggFeatures = logicalPlanPQO.requiredNonWindowAggFeatures
  private val requestedSeqJoinFeatures = logicalPlanPQO.seqJoinFeatures
  private val keyTagIntsToStrings = logicalPlanPQO.keyTagIntsToStrings
  private val allRequiredFeatures = logicalPlanPQO.allRequiredFeatures
  private val allRequestedFeatures = logicalPlanPQO.allRequestedFeatures

  def joinFeaturesAsDF(sparkSession: SparkSession,
                       featureJoinConfig: FeatureJoinConfig,
                       featureGroups: FeatureGroups,
                       keyTaggedFeatureList: Seq[JoiningFeatureParams],
                       observationDF: DataFrame,
                       materializedFeatures: DataFrame,
                       rowBloomFilterThreshold: Option[Int] = None): (DataFrame, Header) = {

    println("=========== Key infos for Feathr FeatureJoin ==========")
    println(s"## user requested features: $keyTaggedFeatureList")
    println(s"## keyTag mapping: ${keyTagIntsToStrings.zipWithIndex}")
    println(s"## resolved dependencies list: $allRequiredFeatures")
    println(s"## join stages: $joinStages, post-join derived features: $postJoinDerivedFeatures")
    println(s"## windowAggFeatures that needs to be computed: $requiredWindowAggFeatures")
    println(s"## non-windowAggFeatures that needs to be computed: $requiredNonWindowAggFeatures")
    println(s"## seqJoin features that needs to be computed: $requestedSeqJoinFeatures")

    val allJoinStages = (windowAggFeatureStages ++ joinStages).distinct
    println("== All Join Stages [Derived + Anchored] ==")
    println(allJoinStages)
    println()

    val useSaltedJoin = FeathrUtils.getFeathrJobParam(
      sparkSession.sparkContext.getConf,
      FeathrUtils.ENABLE_SALTED_JOIN
    ).toBoolean

    val useSlickJoin = !useSaltedJoin && FeathrUtils.getFeathrJobParam(
      sparkSession.sparkContext.getConf,
      FeathrUtils.ENABLE_SLICK_JOIN
    ).toBoolean

    val failOnMissingPartition = FeathrUtils.getFeathrJobParam(
      sparkSession.sparkContext.getConf,
      FeathrUtils.FAIL_ON_MISSING_PARTITION
    ).toBoolean

    println(s"== UseSaltedJoin: ${useSaltedJoin} ==")
    println(s"== UseSlickJoin: ${useSlickJoin} ==")
    println(s"== FailOnMissingPartition: ${failOnMissingPartition} ==")
    println()

    val saltedJoinParameters =
      if (useSaltedJoin) {
        val estimatorType = FrequentItemEstimatorType.withName(
          FeathrUtils.getFeathrJobParam(sparkSession.sparkContext.getConf, FeathrUtils.SALTED_JOIN_FREQ_ITEM_ESTIMATOR)
        )
        val estimator = FrequentItemEstimatorFactory.create(estimatorType)
        val frequentItemThreshold = FeathrUtils.getFeathrJobParam(
          sparkSession.sparkContext.getConf,
          FeathrUtils.SALTED_JOIN_FREQ_ITEM_THRESHOLD
        ).toFloat
        Some(SaltedSparkJoin.JoinParameters(estimator, frequentItemThreshold))
      } else {
        None
      }

    val FeatureDataFrame(withPassthroughFeatureDF, anchoredPassthroughFeatureTypes) =
      joinAnchoredPassthroughFeatures(sparkSession, observationDF, featureGroups)

    val allRequestAnchoredPassthroughFeatures =
      featureGroups.allPassthroughFeatures.filter(
        feature => allRequestedFeatures.map(_.getFeatureName).contains(feature._1)
      ).keySet

    val passthroughFeatureColumns = withPassthroughFeatureDF.columns.diff(observationDF.columns)

    val PreprocessedObservation(bloomFilters, withJoinKeyAndUidObs, origObsWithUid, swaObsTime, extraColumnsInSlickJoin, saltedJoinFrequentItemDFs) =
      PQOUtils.preProcessObservation(
        withPassthroughFeatureDF,
        featureJoinConfig,
        allJoinStages,
        keyTagIntsToStrings,
        rowBloomFilterThreshold,
        saltedJoinParameters,
        passthroughFeatureColumns
    )

    val extraSlickColumns2Remove = withJoinKeyAndUidObs.columns.diff(observationDF.columns).diff(passthroughFeatureColumns)
    // here, obsToJoinWithFeatures is withPassthroughFeatureDF, and is observationDF
    val obsToJoinWithFeatures = if (useSlickJoin) withJoinKeyAndUidObs else withPassthroughFeatureDF

    val requiredRegularFeatureAnchors = requiredNonWindowAggFeatures.filter {
      case ErasedEntityTaggedFeature(_, featureName) =>
        featureGroups.allAnchoredFeatures.contains(featureName) && !featureGroups.allPassthroughFeatures.contains(featureName)
    }.distinct

    val anchorSourceAccessorMap = anchorToDataSourceMapper.getBasicAnchorDFMapForJoin(
      sparkSession,
      requiredRegularFeatureAnchors.map(_.getFeatureName).toIndexedSeq.map(featureGroups.allAnchoredFeatures),
      failOnMissingPartition
    )

    implicit val joinExecContextPQO: JoinExecutionContextPQO =
      JoinExecutionContextPQO(
        sparkSession,
        logicalPlanPQO,
        featureGroups,
        bloomFilters,
        Some(saltedJoinFrequentItemDFs)
      )

    // val FeatureDataFrame(withWindowAggFeatureDF, inferredSWAFeatureTypes) =
    val FeatureDataFrame(withWindowAggFeatureDF, inferredSWAFeatureTypes) =
      joinSWAFeaturesPQO(
        sparkSession,
        obsToJoinWithFeatures,
        featureJoinConfig,
        featureGroups,
        failOnMissingPartition,
        bloomFilters,
        swaObsTime
      )

    // println("== withWindowAggFeatureDF ==")
    // withWindowAggFeatureDF.show()
    // println()

    val anchoredFeatureJoinStepPQO =
      if (useSlickJoin) {
        AnchoredFeatureJoinStepPQO(
          SlickJoinLeftJoinKeyColumnAppender,
          SlickJoinRightJoinKeyColumnAppender,
          SparkJoinWithJoinCondition(EqualityJoinConditionBuilder)
        )
      } else {
        AnchoredFeatureJoinStepPQO(
          SqlTransformedLeftJoinKeyColumnAppender,
          IdentityJoinKeyColumnAppender,
          SparkJoinWithJoinCondition(EqualityJoinConditionBuilder)
        )
      }

    val FeatureDataFrameOutput(FeatureDataFrame(withAllBasicAnchoredFeatureDF, inferredBasicAnchoredFeatureTypes)) =
      anchoredFeatureJoinStepPQO.joinFeatures(
        requiredRegularFeatureAnchors,
        AnchorJoinStepInput(withWindowAggFeatureDF, anchorSourceAccessorMap)
      )

    // we kept the some non join key columns in the trimmed observation (e.g. time stamp column for SWA),
    // we needs to drop before we join to observation, or we will have duplicated column names for join keys
    val withSlickJoinedDF = if (useSlickJoin) {
      val cleanedFeaturesDF = withAllBasicAnchoredFeatureDF.drop(extraColumnsInSlickJoin: _*)
      origObsWithUid
        .join(cleanedFeaturesDF.drop(passthroughFeatureColumns: _*), DataFrameColName.UidColumnName)
        .drop(extraSlickColumns2Remove: _*)
    } else withAllBasicAnchoredFeatureDF

    // 6. Join Derived Features
    val derivedFeatureEvaluator = DerivedFeatureEvaluator(
      ss = sparkSession, featureGroups = featureGroups, dataPathHandlers = dataPathHandlerList
    )
    val derivedFeatureJoinStepPQO = DerivedFeatureJoinStepPQO(derivedFeatureEvaluator)

    val FeatureDataFrameOutput(FeatureDataFrame(withDerivedFeatureDF, inferredDerivedFeatureTypes)) =
      derivedFeatureJoinStepPQO.joinFeatures(
        allRequiredFeatures.filter {
          case ErasedEntityTaggedFeature(_, featureName) => featureGroups.allDerivedFeatures.contains(featureName)
        },
        BaseJoinStepInput(withSlickJoinedDF)
      )

    // println("## SHOW withDerivedFeatureDF DataFrame")
    // withDerivedFeatureDF.show(30)

    // Create a set with only the combination of keyTags and the feature name. This will uniquely determine the column name.
    // If there are multiple delays with a feature alias, then this is treated as a separate feature
    val taggedFeatureSet = keyTaggedFeatureList.map(
      featureParams =>
        if (featureParams.featureAlias.isDefined && featureParams.timeDelay.isDefined) {
          new TaggedFeatureName(featureParams.keyTags.asJava, featureParams.featureAlias.get)
        } else {
          new TaggedFeatureName(featureParams.keyTags.asJava, featureParams.featureName)
        }
    ).toSet

    // 7. Remove unwanted feature columns, e.g, required but not requested features
    val cleanedDF = withDerivedFeatureDF.drop(withDerivedFeatureDF.columns.filter(col => {
      val requested = DataFrameColName
        .getFeatureRefStrFromColumnNameOpt(col)
        .forall { featureRefStr =>
          val featureTags = DataFrameColName.getFeatureTagListFromColumn(col, false)
          // If it is a requested anchored passthrough feature, keep it in the output
          if (allRequestAnchoredPassthroughFeatures.contains(featureRefStr)) true
          else {
            taggedFeatureSet.contains(new TaggedFeatureName(featureTags.asJava, featureRefStr))
          }
        } // preserve all non-feature columns, they are from observation
      !requested
    }): _*)

    // println(s"After removing unwanted columns, cleanedDF:")
    // cleanedDF.show(false)

    val allInferredFeatureTypes = anchoredPassthroughFeatureTypes ++
      inferredBasicAnchoredFeatureTypes ++ inferredSWAFeatureTypes ++ inferredDerivedFeatureTypes

    // 8. Rename the columns to the expected feature name or if an alias is present, to the featureAlias name.
    val taggedFeatureToColumnNameMap = DataFrameColName.getTaggedFeatureToNewColumnName(cleanedDF)

    val taggedFeatureToUpdatedColumnMap: Map[TaggedFeatureName, (String, String)] = taggedFeatureToColumnNameMap.map {
      case (taggedFeatureName, (oldName, newName)) =>
        val featureAlias = getFeatureAlias(keyTaggedFeatureList, taggedFeatureName.getFeatureName, taggedFeatureName.getKeyTag.asScala, None, None)
        if (featureAlias.isDefined) (taggedFeatureName, (oldName, featureAlias.get)) else (taggedFeatureName, (oldName, newName))
    }

    // 9. Get the final dataframe and the headers for every column.
    val (finalDF, header) =
      DataFrameColName.adjustFeatureColNamesAndGetHeader(
        cleanedDF,
        taggedFeatureToUpdatedColumnMap,
        featureGroups.allAnchoredFeatures,
        featureGroups.allDerivedFeatures,
        allInferredFeatureTypes
      )

    // println("Rename Features, finalDF:")
    // finalDF.show(false)

    val finalUnionDF = finalDF.union(materializedFeatures)
    (finalUnionDF, header)

    // (finalDF, header)
  }

  def joinSWAFeaturesPQO(sparkSession: SparkSession,
                         obsToJoinWithFeatures: DataFrame,
                         featureJoinConfig: FeatureJoinConfig,
                         featureGroups: FeatureGroups,
                         failOnMissingPartition: Boolean,
                         bloomFilters: Option[Map[Seq[Int], BloomFilter]],
                         swaObsTime: Option[DateTimeInterval]): FeatureDataFrame = {

    if (windowAggFeatureStages.isEmpty) {
      FeatureDataFrame(obsToJoinWithFeatures, Map())
    }
    else {
      val swaJoinerPQO = new SlidingWindowAggregationJoinerPQO(featureGroups.allWindowAggFeatures, anchorToDataSourceMapper)
      swaJoinerPQO.joinWindowAggFeaturesAsDF(
        sparkSession,
        obsToJoinWithFeatures,
        featureJoinConfig,
        keyTagIntsToStrings,
        windowAggFeatureStages,
        requiredWindowAggFeatures,
        bloomFilters,
        swaObsTime,
        failOnMissingPartition
      )
    }
  }

  def joinAnchoredPassthroughFeatures(sparkSession: SparkSession,
                                      contextDF: DataFrame,
                                      featureGroups: FeatureGroups): FeatureDataFrame = {
    val allAnchoredPassthroughFeatures =
      featureGroups.allPassthroughFeatures.filter(feature => allRequiredFeatures.map(_.getFeatureName).contains(feature._1))

    if (allAnchoredPassthroughFeatures.nonEmpty) {
      //TODO: Handle Pass Through Feature
      println("== AllAnchoredPassthroughFeatures Not Empty ==")
      FeatureDataFrame(contextDF, Map())
    }
    else {
      println("== AllAnchoredPassthroughFeatures Empty ==")
      FeatureDataFrame(contextDF, Map())
    }
  }
}
