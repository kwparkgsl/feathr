package com.linkedin.feathr.offline.pqo

import com.linkedin.feathr.offline.client.DataFrameColName
import com.linkedin.feathr.offline.{FeatureName, JoinStage, KeyTagIdTuple}
import com.linkedin.feathr.offline.config.FeatureJoinConfig
import com.linkedin.feathr.offline.join.DataFrameKeyCombiner
import com.linkedin.feathr.offline.job.DataFrameStatFunctions
import com.linkedin.feathr.offline.join.algorithms.SaltedSparkJoin
import com.linkedin.feathr.offline.swa.SlidingWindowFeatureUtils
import com.linkedin.feathr.offline.util.datetime.DateTimeInterval
import com.linkedin.feathr.offline.util.SourceUtils
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.sketch.BloomFilter

import scala.collection.mutable

private[offline] case class PreprocessedObservation(bloomFilters: Option[Map[Seq[Int], BloomFilter]],
                                                    keyAndUidOnlyDF: DataFrame,
                                                    withUidDF: DataFrame,
                                                    swaObsTime: Option[DateTimeInterval],
                                                    extraColumnsInSlickJoin: Seq[String] = Seq.empty[String],
                                                    saltedJoinFrequentItemDFs: Map[Seq[Int], DataFrame])

private[offline] object PQOUtils {

  val maxExpectedItemForBloomfilter = 50000000L

  val bloomFilterFPP = 0.05

  def preProcessObservation(inputDF: DataFrame,
                            featureJoinConfig: FeatureJoinConfig,
                            joinStages: Seq[JoinStage],
                            keyTagList: Seq[String],
                            rowBloomFilterThreshold: Option[Int],
                            saltedJoinParameters: Option[SaltedSparkJoin.JoinParameters],
                            columnsToPreserve: Seq[String]): PreprocessedObservation = {

    // get event_timestamp range from observation dataset and time type
    val (swaObsTimeRange, obsSWATimeExpr) = SlidingWindowFeatureUtils.getObsSwaDataTimeRange(inputDF, featureJoinConfig.settings)
    val extraColumnsInSlickJoin = getSlidingWindowRelatedColumns(obsSWATimeExpr, joinStages, keyTagList)

    val keyTagsToBloomFilterColumnMap = new mutable.HashMap[Seq[Int], String]

    val (_, withKeyDF, joinKeyColumnNames) = joinStages.foldLeft(keyTagsToBloomFilterColumnMap, inputDF, Seq.empty[String])(
      (accFilterMapDF, joinStage) => {
        val keyTags: Seq[Int] = joinStage._1
        val stringKeyTagList = keyTags.map(keyTagList)
        val accFilterMap = accFilterMapDF._1
        val df = accFilterMapDF._2
        val (bfKeyColName, contextDFWithKeys) = DataFrameKeyCombiner().combine(df, stringKeyTagList)
        accFilterMap.put(keyTags, bfKeyColName)
        (accFilterMap, contextDFWithKeys, accFilterMapDF._3 ++ Seq(bfKeyColName))
      }
    )

    val origJoinKeyColumns = keyTagsToBloomFilterColumnMap.map(_._2).toSeq

    val extraColumns = Seq(DataFrameColName.UidColumnName) ++ extraColumnsInSlickJoin
    val withKeyAndUidDF = withKeyDF.withColumn(DataFrameColName.UidColumnName, monotonically_increasing_id)
    val allSelectedColumns = (extraColumns ++ origJoinKeyColumns ++ columnsToPreserve).distinct

    val keyAndUidOnlyDF = withKeyAndUidDF.select(allSelectedColumns.head, allSelectedColumns.tail: _*)

    val bloomFilters = generateBloomFilters(rowBloomFilterThreshold, withKeyAndUidDF, joinStages, origJoinKeyColumns, keyTagsToBloomFilterColumnMap)
    val withUidDF = withKeyAndUidDF.drop(joinKeyColumnNames: _*)
    val saltedJoinFrequentItemDFs = generateSaltedJoinFrequentItems(keyTagsToBloomFilterColumnMap.toMap, withKeyDF, saltedJoinParameters)

    PreprocessedObservation(
      bloomFilters, keyAndUidOnlyDF, withUidDF, swaObsTimeRange, extraColumnsInSlickJoin, saltedJoinFrequentItemDFs
    )
  }

  def getSlidingWindowRelatedColumns(obsSWATimeExpr: Option[String],
                                     joinStages: Seq[(KeyTagIdTuple, Seq[FeatureName])],
                                     keyTagList: Seq[String]): Seq[String] = {

    // Get the top level fields referenced in a sql expression
    def getTopLevelReferencedFields(sqlExpr: String, ss: SparkSession) = {
      ss.sessionState.sqlParser.parseExpression(sqlExpr).references.map(_.name.split("\\.").head).toSeq
    }

    obsSWATimeExpr.map(timeExpr => {
      val sparkSession: SparkSession = SparkSession.builder().getOrCreate()
      val swaTimeColumns = try {
        getTopLevelReferencedFields(timeExpr, sparkSession)
      } catch {
        case _: Exception => Seq.empty[String]
      }
      val swaJoinKeyColumns = try {
        joinStages.flatMap { joinStage =>
          val keyTags: Seq[Int] = joinStage._1
          keyTags.flatMap(keyTagId => {
            getTopLevelReferencedFields(keyTagList(keyTagId), sparkSession)
          })
        }
      } catch {
        case _: Exception => Seq.empty[String]
      }
      (swaTimeColumns ++ swaJoinKeyColumns).distinct
    }).getOrElse(Seq.empty[String])
  }

  private def generateSaltedJoinFrequentItems(keyTagsToColumnMap: Map[Seq[Int], String],
                                              withKeyDF: DataFrame,
                                              saltedJoinParameters: Option[SaltedSparkJoin.JoinParameters]): Map[Seq[Int], DataFrame] = {
    if (saltedJoinParameters.isEmpty) return Map[Seq[Int], DataFrame]()
    keyTagsToColumnMap.flatMap {
      case (keyTags, keyColumnName) =>
        val frequentItemsDf = SaltedSparkJoin.getFrequentItemsDataFrame(
          saltedJoinParameters.get.estimator,
          withKeyDF,
          keyColumnName,
          saltedJoinParameters.get.frequentItemThreshold)
        if (frequentItemsDf.head(1).nonEmpty) {
          Some(keyTags, frequentItemsDf)
        } else {
          println("Salted Join: no frequent items for key: " + keyColumnName)
          None
        }
    }
  }

  private def generateBloomFilters(rowBloomFilterThreshold: Option[Int],
                                   contextDF: DataFrame,
                                   joinStages: Seq[(KeyTagIdTuple, Seq[FeatureName])],
                                   origJoinKeyColumns: Seq[String],
                                   keyTagsToBloomFilterColumnMap: mutable.HashMap[Seq[Int], String]): Option[Map[Seq[Int], BloomFilter]] = {

    // Join window aggregation features first, separately from all other features.
    val estimatedSize = math.min(SourceUtils.estimateRDDRow(contextDF.rdd), maxExpectedItemForBloomfilter)
    val forceDisable = (rowBloomFilterThreshold.isDefined && rowBloomFilterThreshold.get == 0)

    val generateBloomfilters = if (estimatedSize <= 0) {
      true
    } else {
      val thresholdMet = if (rowBloomFilterThreshold.isEmpty) {
        true
      } else {
        val threshold = rowBloomFilterThreshold.get
        // -1 means force enable
        threshold == -1 || (threshold > 0 && estimatedSize < threshold)
      }
      if (thresholdMet && joinStages.nonEmpty) {
        true
      } else {
        false
      }
    }

    val bloomFilters = if (!forceDisable && generateBloomfilters) {
      // When the estimate is negative which means estimate failed to get a number within acceptable confidence,
      // in this case, we generate bloomfilter with maximum expect item in Feathr set up
      val expectItemNum = if (estimatedSize > 0) estimatedSize else maxExpectedItemForBloomfilter
      val filters = new DataFrameStatFunctions(contextDF).batchCreateBloomFilter(origJoinKeyColumns, expectItemNum, bloomFilterFPP)
      // use map value
      val filterMap = keyTagsToBloomFilterColumnMap.toSeq
        .zip(filters)
        .map {
          case ((tag, _), filter) =>
            (tag, filter)
        }
        .toMap
      Some(filterMap)
    } else None
    bloomFilters
  }
}

