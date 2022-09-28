package com.linkedin.feathr.offline.pqo

import com.linkedin.feathr.common.{FeatureInfo, Header, JoiningFeatureParams, TaggedFeatureName}
import com.linkedin.feathr.common.exception.{ErrorLabel, FeathrConfigException, FeathrException, FeathrInputDataException}
import com.linkedin.feathr.offline.client.DataFrameColName
import com.linkedin.feathr.offline.config.{FeathrConfig, FeathrConfigLoader, FeatureGroupsGenerator, FeatureJoinConfig}
import com.linkedin.feathr.offline.config.sources.FeatureGroupsUpdater
import com.linkedin.feathr.offline.source.accessor.DataPathHandler
import com.linkedin.feathr.offline.job.{FeathrUdfRegistry, JoinJobContext}
import com.linkedin.feathr.offline.join.DataFrameFeatureJoiner
import com.linkedin.feathr.offline.logical.FeatureGroups
import com.linkedin.feathr.offline.pqo.FeatureProvenanceDeserializer.Provenance
import com.linkedin.feathr.offline.util.{AnchorUtils, FeathrUtils, HdfsUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{DataFrame, SparkSession}

class FeathrClientPQO private[offline] (sparkSession: SparkSession,
                                        featureGroups: FeatureGroups,
                                        logicalPlannerPQO: MultiStageJoinPlannerPQO,
                                        featureGroupsUpdater: FeatureGroupsUpdater,
                                        dataPathHandlerList: List[DataPathHandler]) {

  private[offline] val allAnchoredFeatures = featureGroups.allAnchoredFeatures
  private[offline] val allDerivedFeatures = featureGroups.allDerivedFeatures
  private[offline] val allPassthroughFeatures = featureGroups.allPassthroughFeatures
  private[offline] val allWindowAggFeatures = featureGroups.allWindowAggFeatures
  private[offline] val allSeqJoinFeatures = featureGroups.allSeqJoinFeatures

  private[offline] def dispatchFeathrJob(featureJoinConfig: FeatureJoinConfig,
                                         joinJobContext: JoinJobContext,
                                         observationDF: DataFrame): (DataFrame, Header) = {

    println("XX featureJoinConfig: XX")
    println("XXX featureJoinConfig.joinFeatures: XXX")
    println(featureJoinConfig.joinFeatures)
    println("XXX featureJoinConfig.featureGroupings: XXX")
    println(featureJoinConfig.featureGroupings)
    println()
    println("XX joinJobContext: XX")
    println("XXX joinJobContext.feathrFeatureConfig: XXX")
    println(joinJobContext.feathrFeatureConfig)
    println("XXX joinJobContext.inputData: XXX")
    println(joinJobContext.inputData)
    println()
    println("XX allWindowAggFeatures XX")
    for((key, value) <- allWindowAggFeatures) {
      println(s"$key: $value")
    }
    println()

    val feathrJobName = "uc7-sf1-7dt-2dm"

    println("== Start Join Observation And Features using PQO ==")

    val provenanceList = FeatureProvenanceDeserializer.deserializeProvenance()

    var aggWindowProvenance = ""

    provenanceList foreach {
      provenance =>
        provenance.getItems foreach {
          provenanceItem =>
            if(provenanceItem.jobName == feathrJobName) {
              provenanceItem.features foreach {
                featureItem =>
                  if(featureItem.getFeatureType == "anchored_agg") {
                    aggWindowProvenance = featureItem.getAggWindow
                  }
              }
            }
        }
    }

    val joinJobPQOTag = PQOArbitrator.jobArbitrate(featureJoinConfig, provenanceList)
    // val joinJobPQOTag = JobTagPQO.SAME
    println(s"According to provenance, the current job is a ${joinJobPQOTag} job.")

    val (joinedDF, header) = joinJobPQOTag match {
      case JobTagPQO.SAME =>
        val materializedDF = MaterializedFeatureMapper.retrieveMaterializedFeatures(featureJoinConfig, feathrJobName)
        (materializedDF, new Header(Map.empty[TaggedFeatureName, FeatureInfo]))

      case JobTagPQO.SAME_BUT_TIME =>
        val materializedDF = MaterializedFeatureMapper.retrieveMaterializedFeatures(featureJoinConfig, feathrJobName)
        if(!materializedDF.isEmpty) {
          println("Get the materialized features")
          // materializedDF.show()
        }
        // val shouldOptimize = PQOCalibrator.calibrateJobCost(sparkSession, featureGroups, aggWindowProvenance)
        // val materializedDF = sparkSession.emptyDataFrame
        val shouldOptimize = true
        if(shouldOptimize) {
          doJoinObsAndFeaturesPQO(featureJoinConfig, joinJobContext, observationDF, materializedDF)
        } else {
          doJoinObsAndFeatures(featureJoinConfig, joinJobContext, observationDF)
        }

      case JobTagPQO.SIMILAR =>
        JobDetacherPQO.detachJobforPQO()
        (null, null)
        // MaterializedFeatureMapper.retrieveMaterializedFeatures(featureJoinConfig, feathrJobName)
        // PQOCalibrator.calibrateJobCost(featureJoinConfig, joinJobContext)
        // logicalPlannerPQO.getLogicalPlanPQO()

      case JobTagPQO.NEW =>
        doJoinObsAndFeatures(featureJoinConfig, joinJobContext, observationDF)
    }
    joinedDF.show()
    // println(s"Final DataFrames with ${joinedDF.count()} rows and ${joinedDF.columns.length} cols")

    /*
    // write the data frame to redis using redis-lab lib
    joinedDF.write
      .format("org.apache.spark.sql.redis")
      .option("table", feathrJobName)
      .option("host", "localhost")
      .option("port", "6379")
      .option("auth", "123456")
      .save()
    */
    (joinedDF, header)
  }

  private[offline] def doJoinObsAndFeaturesPQO(featureJoinConfig: FeatureJoinConfig,
                                               joinJobContext: JoinJobContext,
                                               observationDF: DataFrame,
                                               materializedDF: DataFrame): (DataFrame, Header) = {
    val featureGroupings = featureJoinConfig.featureGroupings
    val joinFeatures = featureGroupings.values.flatten.toSeq.distinct

    val rowBloomFilterThreshold = FeathrUtils.getFeathrJobParam(
      sparkSession,
      FeathrUtils.ROW_BLOOMFILTER_MAX_THRESHOLD
    ).toInt

    val (joinedDF, header) = {
      if (featureGroupings.isEmpty) {
        //  just return observationDF and empty Header
        println("Feature groupings from the join config is empty, returning the obs data without joining any features.")
        (observationDF, new Header(Map.empty[TaggedFeatureName, FeatureInfo]))
      } else {
        println("## Start joinFeaturesAsDFbasePQO Function")
        joinFeaturesAsDFbasePQO(
          featureJoinConfig,
          joinFeatures,
          observationDF,
          materializedDF,
          Some(rowBloomFilterThreshold)
        )
      }
    }
    (joinedDF, header)
  }

  private[offline] def joinFeaturesAsDFbasePQO(featureJoinConfig: FeatureJoinConfig,
                                               keyTaggedFeatures: Seq[JoiningFeatureParams],
                                               left: DataFrame,
                                               materializedDF: DataFrame,
                                               rowBloomFilterThreshold: Option[Int] = None): (DataFrame, Header) = {
    println("Start Joining Features as DataFrame based on PQO")

    // check the first row of left, i.e., observationDF, to check if it is empty
    // if so, directly return observationDF and an empty header
    if (left.head(1).isEmpty) {
      println("Observation Dataset is empty")
      return (left, new Header(Map.empty[TaggedFeatureName, FeatureInfo]))
    }

    val updatedFeatureGroups = featureGroupsUpdater.updateFeatureGroups(featureGroups, keyTaggedFeatures)

    println("XX keyTaggedFeatures XX")
    println(keyTaggedFeatures)
    println()

    val logicalPlanPQO = logicalPlannerPQO.getLogicalPlanPQO(updatedFeatureGroups, keyTaggedFeatures)

    val invalidFeatureNames = findInvalidFeatureRefs(keyTaggedFeatures)

    if (invalidFeatureNames.nonEmpty) {
      throw new FeathrInputDataException(
        ErrorLabel.FEATHR_USER_ERROR,
        s"Feature names must conform: ${AnchorUtils.featureNamePattern}, but found feature names: $invalidFeatureNames")
    }

    val conflictFeatureNames: Seq[String] = keyTaggedFeatures.map(_.featureName).intersect(left.schema.fieldNames)
    if (conflictFeatureNames.nonEmpty) {
      throw new FeathrConfigException(
        ErrorLabel.FEATHR_USER_ERROR,
        "Feature names must be different from field names in the observation data. " +
          s"Please rename feature ${conflictFeatureNames} or rename the same field names in the observation data.")
    }

    val joinerPQO = new DataFrameFeatureJoinerPQO(logicalPlanPQO, dataPathHandlerList)
    joinerPQO.joinFeaturesAsDF(
      sparkSession,
      featureJoinConfig,
      updatedFeatureGroups,
      keyTaggedFeatures,
      left,
      materializedDF,
      rowBloomFilterThreshold
    )
  }

  private[offline] def doJoinObsAndFeatures(featureJoinConfig: FeatureJoinConfig,
                                            joinJobContext: JoinJobContext,
                                            observationDF: DataFrame): (DataFrame, Header) = {
    // show the name
    println("All anchored feature names (sorted):\n\t" + stringifyFeatureNames(allAnchoredFeatures.keySet))
    println("All derived feature names (sorted):\n\t" + stringifyFeatureNames(allDerivedFeatures.keySet))
    SQLConf.get.setConfString("spark.sql.legacy.allowUntypedScalaUDF", "true")

    // enableCheckPoint is false by default
    val enableCheckPoint = FeathrUtils.getFeathrJobParam(sparkSession, FeathrUtils.ENABLE_CHECKPOINT).toBoolean
    val checkpointDir = FeathrUtils.getFeathrJobParam(sparkSession, FeathrUtils.CHECKPOINT_OUTPUT_PATH)
    if (enableCheckPoint) {
      if (checkpointDir.equals("")) {
        throw new FeathrException(
          ErrorLabel.FEATHR_USER_ERROR,
          s"Please set ${FeathrUtils.FEATHR_PARAMS_PREFIX}${FeathrUtils.CHECKPOINT_OUTPUT_PATH} to a folder with write permission.")
      }
      // clean up old checkpoints
      HdfsUtils.deletePath(checkpointDir)
      sparkSession.sparkContext.setCheckpointDir(checkpointDir)
    }

    val rowBloomFilterThreshold = FeathrUtils.getFeathrJobParam(
      sparkSession,
      FeathrUtils.ROW_BLOOMFILTER_MAX_THRESHOLD
    ).toInt

    val featureGroupings = featureJoinConfig.featureGroupings
    val joinFeatures = featureGroupings.values.flatten.toSeq.distinct

    val (joinedDF, header) = {
      if (featureGroupings.isEmpty) {
        println("Feature groupings from the join config is empty, returning the obs data without joining any features.")
        (observationDF, new Header(Map.empty[TaggedFeatureName, FeatureInfo]))
      } else {
        println("## Start joinFeaturesAsDF Function")
        joinFeaturesAsDF(featureJoinConfig, joinFeatures, observationDF, Some(rowBloomFilterThreshold))
      }
    }

    (joinedDF, header)
  }

  private[offline] def joinFeaturesAsDF(featureJoinConfig: FeatureJoinConfig,
                                        keyTaggedFeatures: Seq[JoiningFeatureParams],
                                        left: DataFrame,
                                        rowBloomFilterThreshold: Option[Int] = None): (DataFrame, Header) = {
    println("Start Joining Features as DataFrame")

    // check the first row of left, i.e., observationDF, to check if it is empty
    // if so, directly return observationDF and an empty header
    if(left.head(1).isEmpty) {
      println("Observation Dataset is empty")
      return (left, new Header(Map.empty[TaggedFeatureName, FeatureInfo]))
    }

    val updatedFeatureGroups = featureGroupsUpdater.updateFeatureGroups(featureGroups, keyTaggedFeatures)

    val logicalPlan = logicalPlannerPQO.getLogicalPlan(updatedFeatureGroups, keyTaggedFeatures)

    val invalidFeatureNames = findInvalidFeatureRefs(keyTaggedFeatures)

    if (invalidFeatureNames.nonEmpty) {
      throw new FeathrInputDataException(
        ErrorLabel.FEATHR_USER_ERROR,
        "Feature names must conform to " +
          s"regular expression: ${AnchorUtils.featureNamePattern}, but found feature names: $invalidFeatureNames")
    }

    val conflictFeatureNames: Seq[String] = keyTaggedFeatures.map(_.featureName).intersect(left.schema.fieldNames)
    if (conflictFeatureNames.nonEmpty) {
      throw new FeathrConfigException(
        ErrorLabel.FEATHR_USER_ERROR,
        "Feature names must be different from field names in the observation data. " +
          s"Please rename feature ${conflictFeatureNames} or rename the same field names in the observation data.")
    }

    val joiner = new DataFrameFeatureJoiner(logicalPlan=logicalPlan, dataPathHandlerList)
    joiner.joinFeaturesAsDF(
      sparkSession, featureJoinConfig, updatedFeatureGroups, keyTaggedFeatures, left, rowBloomFilterThreshold
    )
  }

  private def findConflictFeatureNames(keyTaggedFeatures: Seq[JoiningFeatureParams],
                                       fieldNames: Seq[String]): Seq[String] = {
    keyTaggedFeatures.map(_.featureName).intersect(fieldNames)
  }

  private def stringifyFeatureNames(nameSet: Set[String]): String = {
    nameSet.toSeq.sorted.toArray.mkString("\n\t")
  }

  private def findInvalidFeatureRefs(features: Seq[JoiningFeatureParams]): List[String] = {
    features.foldLeft(List.empty[String]) {
      (acc, f) =>
        val featureRefStr = f.featureName
        val featureRefStrInDF = DataFrameColName.getEncodedFeatureRefStrForColName(featureRefStr)
        val isValidSyntax = AnchorUtils.featureNamePattern.matcher(featureRefStrInDF).matches()
        if (isValidSyntax) acc
        else featureRefStr :: acc
    }
  }
}

object FeathrClientPQO {
  def builder(sparkSession: SparkSession): Builder = {
    FeathrUdfRegistry.registerUdf(sparkSession)
    new Builder(sparkSession)
  }

  class Builder private[FeathrClientPQO] (sparkSession: SparkSession) {
    private val feathrConfigLoader = FeathrConfigLoader()
    private var featureDefList: List[String] = List()
    private var localOverrideDefList: List[String] = List()
    private var featureDefPathList: List[String] = List()
    private var localOverrideDefPathList: List[String] = List()
    private var featureDefConfList: List[FeathrConfig] = List()
    private var dataPathHandlerList: List[DataPathHandler] = List()

    def build(): FeathrClientPQO = {
      require(
        localOverrideDefPathList.nonEmpty
          || localOverrideDefList.nonEmpty
          || featureDefList.nonEmpty
          || featureDefPathList.nonEmpty
          || featureDefConfList.nonEmpty,
        "Cannot build feathrClient without a feature def conf file/string or local override def conf file/string")

      var featureDefConfigs = List.empty[FeathrConfig]
      var localDefConfigs = List.empty[FeathrConfig]

      if (featureDefPathList.nonEmpty) {
        featureDefPathList foreach(
          path => readHdfsFile(Some(path)).foreach(
            cfg => featureDefConfigs = feathrConfigLoader.load(cfg) :: featureDefConfigs
          ))
      }

      if (featureDefList.nonEmpty) {
        featureDefList foreach(confStr => featureDefConfigs = feathrConfigLoader.load(confStr) :: featureDefConfigs)
      }

      if (localOverrideDefPathList.nonEmpty) {
        localOverrideDefPathList foreach(
          path => readHdfsFile(Some(path)).foreach(
            cfg => localDefConfigs = feathrConfigLoader.load(cfg) :: localDefConfigs
          ))
      }

      if (localOverrideDefList.nonEmpty) {
        localOverrideDefList foreach(confStr => localDefConfigs = feathrConfigLoader.load(confStr) :: localDefConfigs)
      }

      featureDefConfigs = featureDefConfigs ++ featureDefConfList

      // it is essentially a map, feature_name -> feature_definition
      val featureGroups = FeatureGroupsGenerator(featureDefConfigs, Some(localDefConfigs)).getFeatureGroups()

      // create FeathrClientPQO using new MultiStageJoinPlannerPQO
      val feathrClientPQO =
        new FeathrClientPQO(
          sparkSession,
          featureGroups,
          MultiStageJoinPlannerPQO(),
          FeatureGroupsUpdater(),
          dataPathHandlerList
        )

      feathrClientPQO
    }

    private[offline] def readHdfsFile(path: Option[String]): Option[String] = {
      path.map(p => sparkSession.sparkContext.textFile(p).collect.mkString("\n"))
    }

    def addDataPathHandlerList(dataPathHandlerList: List[DataPathHandler]): Builder = {
      // append the new dataPathHandlers to the exising one
      this.dataPathHandlerList = dataPathHandlerList ++ this.dataPathHandlerList
      this
    }

    def addDataPathHandler(dataPathHandler: DataPathHandler): Builder = {
      this.dataPathHandlerList = dataPathHandler :: this.dataPathHandlerList
      this
    }

    def addDataPathHandler(dataPathHandler: Option[DataPathHandler]): Builder = {
      if (dataPathHandler.isDefined) addDataPathHandler(dataPathHandler.get) else this
    }

    def addFeatureDef(featureDef: String): Builder = {
      this.featureDefList = featureDef :: this.featureDefList
      this
    }

    def addFeatureDef(featureDef: Option[String]): Builder = {
      if (featureDef.isDefined) addFeatureDef(featureDef.get) else this
    }

    def addLocalOverrideDef(localOverrideDef: String): Builder = {
      this.localOverrideDefList = localOverrideDef :: this.localOverrideDefList
      this
    }

    def addLocalOverrideDef(localOverrideDef: Option[String]): Builder = {
      if (localOverrideDef.isDefined) addFeatureDef(localOverrideDef.get) else this
    }

    def addLocalOverrideDefPath(localOverrideDefPath: String): Builder = {
      this.localOverrideDefPathList = localOverrideDefPath :: this.localOverrideDefPathList
      this
    }

    def addLocalOverrideDefPath(localOverrideDefPath: Option[String]): Builder = {
      if (localOverrideDefPath.isDefined) addLocalOverrideDefPath(localOverrideDefPath.get) else this
    }

    def addFeatureDefPath(featureDefPath: String): Builder = {
      this.featureDefPathList = featureDefPath :: this.featureDefPathList
      this
    }

    def addFeatureDefPath(featureDefPath: Option[String]): Builder = {
      if (featureDefPath.isDefined) addFeatureDefPath(featureDefPath.get) else this
    }

  }
}
