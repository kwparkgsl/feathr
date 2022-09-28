package com.linkedin.feathr.offline.pqo

import com.linkedin.feathr.common.FeatureTypeConfig
import com.linkedin.feathr.common.exception.{ErrorLabel, FeathrConfigException}
import com.linkedin.feathr.offline.anchored.WindowTimeUnit
import com.linkedin.feathr.offline.anchored.anchorExtractor.TimeWindowConfigurableAnchorExtractor
import com.linkedin.feathr.offline.{FeatureDataFrame, JoinStage}
import com.linkedin.feathr.offline.anchored.feature.FeatureAnchorWithSource
import com.linkedin.feathr.offline.anchored.keyExtractor.{MVELSourceKeyExtractor, SQLSourceKeyExtractor}
import com.linkedin.feathr.offline.client.DataFrameColName
import com.linkedin.feathr.offline.config.FeatureJoinConfig
import com.linkedin.feathr.offline.exception.FeathrIllegalStateException
import com.linkedin.feathr.offline.job.PreprocessedDataFrameManager
import com.linkedin.feathr.offline.join.DataFrameKeyCombiner
import com.linkedin.feathr.offline.swa.SlidingWindowFeatureUtils
import com.linkedin.feathr.offline.transformation.AnchorToDataSourceMapper
import com.linkedin.feathr.offline.transformation.DataFrameDefaultValueSubstituter.substituteDefaults
import com.linkedin.feathr.offline.util.FeathrUtils
import com.linkedin.feathr.offline.util.datetime.DateTimeInterval
import com.linkedin.feathr.swj.{LabelData, SlidingWindowJoin}
import com.linkedin.feathr.{common, offline}
import org.apache.spark.sql.functions.{col, lit, min, to_date, date_sub}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.util.sketch.BloomFilter

import scala.collection.mutable


private[offline] class SlidingWindowAggregationJoinerPQO(allWindowAggFeatures: Map[String, FeatureAnchorWithSource],
                                                         anchorToDataSourceMapper: AnchorToDataSourceMapper) {
  def joinWindowAggFeaturesAsDF(sparkSession: SparkSession,
                                obsDF: DataFrame,
                                joinConfig: FeatureJoinConfig,
                                keyTagList: Seq[String],
                                windowAggFeatureStages: Seq[JoinStage],
                                requiredWindowAggFeatures: Seq[common.ErasedEntityTaggedFeature],
                                bloomFilters: Option[Map[Seq[Int], BloomFilter]],
                                swaObsTimeOpt: Option[DateTimeInterval],
                                failOnMissingPartition: Boolean): FeatureDataFrame = {

    val joinConfigSettings = joinConfig.settings

    if (joinConfigSettings.isEmpty) {
      throw new FeathrConfigException(
        ErrorLabel.FEATHR_USER_ERROR,
        "settings section are not defined in join config, cannot extract observation data time range"
      )
    }

    if (joinConfigSettings.get.joinTimeSetting.isEmpty) {
      throw new FeathrConfigException(
        ErrorLabel.FEATHR_USER_ERROR,
        "joinTimeSettings section is not defined in join config," +
          " cannot perform window aggregation operation")
    }

    val enableCheckPoint = FeathrUtils.getFeathrJobParam(sparkSession, FeathrUtils.ENABLE_CHECKPOINT).toBoolean

    val timeWindowJoinSettings = joinConfigSettings.get.joinTimeSetting.get

    val simulatedDelay = timeWindowJoinSettings.simulateTimeDelay

    if (simulatedDelay.isEmpty && joinConfig.featuresToTimeDelayMap.nonEmpty) {
      throw new FeathrConfigException(
        ErrorLabel.FEATHR_USER_ERROR,
        "overrideTimeDelay cannot be defined without setting a simulateTimeDelay in the " +
          "joinTimeSettings")
    }

    val featuresToDelayImmutableMap: Map[String, java.time.Duration] =
      joinConfig.featuresToTimeDelayMap.mapValues(WindowTimeUnit.parseWindowTime) ++
        simulatedDelay.map(SlidingWindowFeatureUtils.DEFAULT_TIME_DELAY -> _).toMap

    // find the window aggregate feature anchor configs
    val windowAggFeatureNames = requiredWindowAggFeatures.map(_.getFeatureName).toIndexedSeq
    val windowAggAnchors = windowAggFeatureNames.map(allWindowAggFeatures)

    var contextDF: DataFrame = obsDF

    val swaObsTimeRange = swaObsTimeOpt.get

    val windowAggSourceToAnchor = windowAggAnchors.map(anchor => {
      val featureNames = PreprocessedDataFrameManager.getPreprocessingUniquenessForAnchor(anchor)
      ((anchor.source, anchor.featureAnchor.sourceKeyExtractor.toString(), featureNames), anchor)
    })
      .groupBy(_._1)
      .map({
        case (source, grouped) => (source, grouped.map(_._2))
      })

    val windowAggAnchorDFMap = windowAggSourceToAnchor.flatMap({
      case (sourceWithKeyExtractor, anchors) =>
        val maxDurationPerSource = anchors
          .map(SlidingWindowFeatureUtils.getMaxWindowDurationInAnchor(_, windowAggFeatureNames))
          .max

        println(s"## Selected max window duration $maxDurationPerSource")
        println(s"## across all anchors for source ${sourceWithKeyExtractor._1.path}")
        println()
        // use preprocessed DataFrame if it exist. Otherwise use the original source DataFrame.
        // there are might be duplicates, for example:
        // Vector(f_location_avg_fare, f_location_max_fare, f_location_avg_fare, f_location_max_fare)
        val res = anchors.flatMap(x => x.featureAnchor.features)
        val featureNames = res.toSet.toSeq.sorted.mkString(",")

        val preprocessedDf = PreprocessedDataFrameManager.preprocessedDfMap.get(featureNames)

        val originalSourceDf = anchorToDataSourceMapper.getWindowAggAnchorDFMapForJoin(
          sparkSession,
          sourceWithKeyExtractor._1,
          swaObsTimeRange,
          maxDurationPerSource,
          featuresToDelayImmutableMap.values.toArray,
          failOnMissingPartition
        )

        val sourceDF: DataFrame = preprocessedDf match {
          case Some(existDf) => existDf
          case None => originalSourceDf
        }

        // println("## SHOW sourceDF DataFrame")
        // sourceDF.show()

        // all the anchors here have same key sourcekey extractor
        // so we just use the first one to generate key column and share
        // the lateral view parameter in sliding window aggregation feature is handled differently in feature join
        // and feature generation i.e., in feature generation, they are handled in key source extractor,
        // in feature join here, we don't handle lateralView parameter in key extractor, and leave it to Spark SWJ library
        val withKeyDF = anchors.head.featureAnchor.sourceKeyExtractor match {
          case keyExtractor: SQLSourceKeyExtractor =>
            keyExtractor.appendKeyColumns(sourceDF, evaluateLateralViewParams = false)
          case keyExtractor =>
            keyExtractor.appendKeyColumns(sourceDF)
        }

        val Row(min_ts: String) = contextDF.agg(min("event_timestamp")).head()
        println(s"## MIN TIMESTAMP: ${min_ts}")

        /*
        println("## SHOW withKeyDF")
        withKeyDF.show()
        println(s"Final DataFrames with ${withKeyDF.count()} rows and ${withKeyDF.columns.length} cols")
        println()
        */
        val duration_days = maxDurationPerSource.toDays.toInt
        /*
        val distinctExprs = withKeyDF.columns.map(_ -> "approx_count_distinct").toMap
        val withKeyDistinctDF = withKeyDF.agg(distinctExprs)
        withKeyDF.columns.toList.foreach {
          columnName =>
            val columnDistinctName = "approx_count_distinct(" + columnName + ")"
            val distinctNum = withKeyDistinctDF.select(withKeyDistinctDF(columnDistinctName)).head().getLong(0)
            println(s"Cardinality of ${columnName}: ${distinctNum}")
        }

        println()

        val withKeyFilterDF = withKeyDF.filter(withKeyDF("order_date").geq(date_sub(lit(min_ts), 30)))
        val filterDistinctExprs = withKeyFilterDF.columns.map(_ -> "approx_count_distinct").toMap
        val withKeyFilterDistinctDF = withKeyFilterDF.agg(filterDistinctExprs)

        withKeyFilterDF.columns.toList.foreach {
          columnName =>
            val columnDistinctName = "approx_count_distinct(" + columnName + ")"
            val distinctNum = withKeyFilterDistinctDF.select(withKeyFilterDistinctDF(columnDistinctName)).head().getLong(0)
            println(s"Cardinality of ${columnName}: ${distinctNum}")
        }
        */
        // println(s"Final DataFrames with ${withKeyFilterDF.count()} rows and ${withKeyFilterDF.columns.length} cols")

        val withKeyFilterDF = withKeyDF.filter(withKeyDF("order_date").geq(date_sub(lit(min_ts), 30)))

        // println("## withKeyFilterDF")
        // withKeyFilterDF.show(30)

        anchors.map(anchor => (anchor, withKeyFilterDF))
    })

    val allInferredFeatureTypes = mutable.Map.empty[String, FeatureTypeConfig]

    windowAggFeatureStages.foreach({
      case (keyTags: Seq[Int], featureNames: Seq[String]) =>
        val stringKeyTags = keyTags.map(keyTagList).map(k => s"CAST (${k} AS string)")

        // get the bloom filter for the key combinations in this stage
        val bloomFilter = bloomFilters match {
          case Some(filters) => Option(filters(keyTags))
          case None => None
        }

        // If there is no joinTimeSettings, then we will assume it is useLatestFeatureData
        val timeStampExpr = if (!timeWindowJoinSettings.useLatestFeatureData) {
          SlidingWindowFeatureUtils.constructTimeStampExpr(
            timeWindowJoinSettings.timestampColumn.name,
            timeWindowJoinSettings.timestampColumn.format
          )
        } else {
          "unix_timestamp()" // if useLatestFeatureData=true, return the current unix timestamp (w.r.t UTC time zone)
        }

        val labelDataDef = LabelData(contextDF, stringKeyTags, timeStampExpr)

        val windowAggAnchorsThisStage = featureNames.map(allWindowAggFeatures)
        val windowAggAnchorDFThisStage = windowAggAnchorDFMap.filterKeys(windowAggAnchorsThisStage.toSet)

        val factDataDefs =
          SlidingWindowFeatureUtils.getSWAAnchorGroups(windowAggAnchorDFThisStage).map {
            anchorWithSourceToDFMap =>
              val selectedFeatures =
                anchorWithSourceToDFMap.keySet.flatMap(_.selectedFeatures).filter(featureNames.contains(_))

              val factData = anchorWithSourceToDFMap.head._2

              val anchor = anchorWithSourceToDFMap.head._1
              val filteredFactData = bloomFilter match {
                case None => factData // no bloom filter: use data as it
                case Some(filter) =>
                  // get the list of join keys.
                  if (anchor.featureAnchor.sourceKeyExtractor.isInstanceOf[MVELSourceKeyExtractor]) {
                    throw new FeathrConfigException(
                      ErrorLabel.FEATHR_USER_ERROR,
                      "MVELSourceKeyExtractor is not supported in sliding window aggregation"
                    )
                  }
                  val keyColumnsList = anchor.featureAnchor.sourceKeyExtractor.getKeyColumnNames(None)
                  // generate the same concatenated-key column for bloom filtering
                  val (bfFactKeyColName, factDataWithKeys) =
                    DataFrameKeyCombiner().combine(factData, keyColumnsList)
                  // use bloom filter on generated concat-key column
                  val filtered = factDataWithKeys.filter(
                    SlidingWindowFeatureUtils.mightContain(filter)(col(bfFactKeyColName))
                  )
                  // remove the concat-key column
                  filtered.drop(col(bfFactKeyColName))
              }

              SlidingWindowFeatureUtils.getFactDataDef(
                filteredFactData,
                anchorWithSourceToDFMap.keySet.toSeq,
                featuresToDelayImmutableMap,
                selectedFeatures
              )
          }
        val origContextObsColumns = labelDataDef.dataSource.columns

        contextDF = SlidingWindowJoin.join(labelDataDef, factDataDefs.toList)

        val defaults = windowAggAnchorDFThisStage.flatMap(s => s._1.featureAnchor.defaults)
        val userSpecifiedTypesConfig = windowAggAnchorDFThisStage.flatMap(_._1.featureAnchor.featureTypeConfigs)

        // Create a map from the feature name to the column format, ie - RAW or FDS_TENSOR
        val featureNameToColumnFormat = allWindowAggFeatures.map(
          nameToFeatureAnchor => nameToFeatureAnchor._1 -> nameToFeatureAnchor._2.featureAnchor.extractor
            .asInstanceOf[TimeWindowConfigurableAnchorExtractor].features(nameToFeatureAnchor._1).columnFormat
        )

        val FeatureDataFrame(withFDSFeatureDF, inferredTypes) =
          SlidingWindowFeatureUtils.convertSWADFToFDS(
            contextDF,
            featureNames.toSet,
            featureNameToColumnFormat,
            userSpecifiedTypesConfig
          )

        // apply default on FDS dataset
        val withFeatureContextDF =
          substituteDefaults(
            withFDSFeatureDF,
            defaults.keys.filter(featureNames.contains).toSeq,
            defaults,
            userSpecifiedTypesConfig,
            sparkSession
          )

        allInferredFeatureTypes ++= inferredTypes

        contextDF =
          standardizeFeatureColumnNames(
            origContextObsColumns,
            withFeatureContextDF,
            featureNames,
            keyTags.map(keyTagList)
          )

        if (enableCheckPoint) {
          // checkpoint complicated dataframe for each stage to avoid Spark failure
          contextDF = contextDF.checkpoint(true)
        }

    })
    offline.FeatureDataFrame(contextDF, allInferredFeatureTypes.toMap)

  }

  def standardizeFeatureColumnNames(origContextObsColumns: Seq[String],
                                    withSWAFeatureDF: DataFrame,
                                    featureNames: Seq[String],
                                    keyTags: Seq[String]): DataFrame = {
    val inputColumnSize = origContextObsColumns.size
    val outputColumnNum = withSWAFeatureDF.columns.length
    if (outputColumnNum != inputColumnSize + featureNames.size) {
      throw new FeathrIllegalStateException(
        s"Number of columns (${outputColumnNum}) in the dataframe returned by " +
          s"sliding window aggregation does not equal to number of columns in the observation data (${inputColumnSize}) " +
          s"+ number of features (${featureNames.size}). Columns in returned dataframe are ${withSWAFeatureDF.columns}," +
          s" columns in observation dataframe are ${origContextObsColumns}")
    }

    /*
     * SWA feature column name returned here is just the feature names, need to replace SWA feature column name
     * following new feature column naming convention (same as non-SWA feature names)
     */
    val renamingPairs = featureNames map { feature =>
      val columnName = DataFrameColName.genFeatureColumnName(feature, Some(keyTags))
      (feature, columnName)
    }

    renamingPairs.foldLeft(withSWAFeatureDF) {
      (baseDF, renamePair) => baseDF.withColumnRenamed(renamePair._1, renamePair._2)
    }
  }
}
