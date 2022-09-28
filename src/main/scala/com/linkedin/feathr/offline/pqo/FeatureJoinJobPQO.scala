package com.linkedin.feathr.offline.pqo

import com.linkedin.feathr.common.exception.{ErrorLabel, FeathrDataOutputException, FeathrInputDataException}
import com.linkedin.feathr.common.{Header, JoiningFeatureParams}
import com.linkedin.feathr.offline.config.datasource.{DataSourceConfigUtils, DataSourceConfigs}
import com.linkedin.feathr.offline.util.{AclCheckUtils, CmdLineParser, FeathrUtils, HdfsUtils, OptionParam, SourceUtils}
import com.linkedin.feathr.offline.job.{FeathrUdfRegistry, JoinJobContext, LocalTestConfig, PreprocessedDataFrameManager}
import com.linkedin.feathr.offline.source.SourceFormatType
import com.linkedin.feathr.offline.source.accessor.DataPathHandler
import com.linkedin.feathr.offline.client.InputData
import com.linkedin.feathr.offline.config.FeatureJoinConfig
import com.linkedin.feathr.offline.source.dataloader.DataLoaderHandler
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.linkedin.feathr.offline.config.location.{DataLocation, SimplePath}
import com.linkedin.feathr.offline.generation.SparkIOUtils
import com.linkedin.feathr.offline.pqo.JobTagPQO.JobTagPQO
import com.linkedin.feathr.offline.transformation.AnchorToDataSourceMapper
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.commons.cli.{Option => CmdOption}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable
import collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object FeatureJoinJobPQO {

  def loadSourceDataframe(args: Array[String],
                          featureNamesInAnchorSet: java.util.Set[String]): java.util.Map[String, DataFrame] = {
    println(s"FeatureJoinJob args are: ${args.mkString}")
    println(s"Feature join job [loadDataframe]: $featureNamesInAnchorSet")
    val feathrJoinPreparationInfo = prepareSparkSession(args)
    val sparkSession = feathrJoinPreparationInfo.sparkSession
    val hadoopConf = feathrJoinPreparationInfo.hadoopConf
    val jobContext = feathrJoinPreparationInfo.feathrJoinJobContext

    // check read authorization for observation data, and write authorization for output path
    checkAuthorization(sparkSession, hadoopConf, jobContext, List())

    // Doesn't support loading local test client for this yet
    val feathrClientPQO= getFeathrClient(sparkSession, jobContext.joinJobContext, List()) //TODO: fix python errors for loadSourceDataFrame, add dataPathLoader Support

    val allAnchoredFeatures = feathrClientPQO.allAnchoredFeatures

    // Using AnchorToDataSourceMapper to load DataFrame for preprocessing
    val failOnMissing = FeathrUtils.getFeathrJobParam(sparkSession, FeathrUtils.FAIL_ON_MISSING_PARTITION).toBoolean
    val anchorToDataSourceMapper = new AnchorToDataSourceMapper(List()) //TODO: fix python errors for loadSourceDataFrame, add dataPathLoader Support
    val anchorsWithSource = anchorToDataSourceMapper.getBasicAnchorDFMapForJoin(
      sparkSession,
      allAnchoredFeatures.values.toSeq,
      failOnMissing)

    // Only load DataFrames for anchors that have preprocessing UDF
    // So we filter out anchors that doesn't have preprocessing UDFs
    // We use feature names sorted and merged as the key to find the anchor
    // For example, f1, f2 belongs to anchor. Then Map("f1,f2"-> anchor)
    val dataFrameMapForPreprocessing = anchorsWithSource
      .filter(x => featureNamesInAnchorSet.contains(x._1.featureAnchor.features.toSeq.sorted.mkString(",")))
      .map(x => (x._1.featureAnchor.features.toSeq.sorted.mkString(","), x._2.get()))

    // Pyspark only understand Java map so we need to convert Scala map back to Java map.
    dataFrameMapForPreprocessing.asJava
  }

  def mainWithPreprocessedDataFrame(args: Array[String], preprocessedDfMap: java.util.Map[String, DataFrame]) {
    // Set the preprocessed DataFrame here for future usage.
    PreprocessedDataFrameManager.preprocessedDfMap = preprocessedDfMap.asScala.toMap

    main(args)
  }

  def main(args: Array[String]): Unit = {
    println(s"=== FeatureJoinJob args are: ===")
    for(e <- args) println(e)
    println("=========================")

    val feathrJoinPreparationInfo = prepareSparkSession(args)

    run(
      feathrJoinPreparationInfo.sparkSession,
      feathrJoinPreparationInfo.hadoopConf,
      feathrJoinPreparationInfo.feathrJoinJobContext,
      List()
    )

    sys.exit(0)
  }

  def prepareSparkSession(args: Array[String]): FeathrJoinPreparationInfo = {
    val feathrJoinJobContext = parseInputArgument(args)

    val sparkConf = new SparkConf().registerKryoClasses(Array(classOf[GenericRecord]))

    val sparkSessionBuilder = SparkSession
      .builder()
      .config(sparkConf)
      .appName(getClass.getName)
      .enableHiveSupport()

    val sparkSession = sparkSessionBuilder.getOrCreate()
    // get all the hdfs configuration files, like core-default.xml, core-site.xml
    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration

    DataSourceConfigUtils.setupHadoopConf(sparkSession, feathrJoinJobContext.dataSourceConfigs)

    // register some UDFs like cast_float, cast_int
    FeathrUdfRegistry.registerUdf(sparkSession)
    // delete the give path to make sure the outputPath is clean
    feathrJoinJobContext.joinJobContext.outputPath match {
      case SimplePath(path) => HdfsUtils.deletePath(path, recursive = true, hadoopConf)
      case _ => {}
    }

    val enableDebugLog = FeathrUtils.getFeathrJobParam(sparkConf, FeathrUtils.ENABLE_DEBUG_OUTPUT).toBoolean
    if (enableDebugLog) {
      Logger.getRootLogger.setLevel(Level.DEBUG)
    }

    FeathrJoinPreparationInfo(sparkSession, hadoopConf, feathrJoinJobContext)
  }

  def parseInputArgument(args: Array[String]): FeathrJoinJobContext = {
    // option long name, short name, description, arg name (null means not argument), default value (null means required)
    val params = Map(
      "feathr-config" -> OptionParam("f", "Path of the feathr local config file", "FCONF", ""),
      "feature-config" -> OptionParam("ef", "Names of the feathr feature config files", "EFCONF", ""),
      "local-override-all" -> OptionParam("loa", "Local config overrides all other configs", "LOCAL_OVERRIDE", "true"),
      "join-config" -> OptionParam("j", "Path of the join config file", "JCONF", ""),
      "input" -> OptionParam("i", "Path of the input data set", "INPUT", ""),
      "output" -> OptionParam("o", "Path of the output", "OUTPUT", ""),
      "num-parts" -> OptionParam("n", "Number of output part files", "NPARTS", "-1"),
      "pass-through-field" -> OptionParam("p", "Pass-through feature field name", "PFIELD", ""),
      "pass-through-features" -> OptionParam("t", "Pass-through feature list, comma-separated", "PLIST", ""),
      "source-type" -> OptionParam("st", "Source type of the observation data", "SRCTYPE", "FIXED_PATH"),
      "start-date" -> OptionParam("sd", "Start date of the observation data if it's time based", "SDATE", ""),
      "end-date" -> OptionParam("ed", "End date of the observation data if it's time based", "EDATE", ""),
      "num-days" -> OptionParam("nd", "Number of days before the offset date if it's time based", "NDAYS", ""),
      "date-offset" -> OptionParam("do", "Offset of observation data if it's time based", "DOFFSET", ""),
      "join-parallelism" -> OptionParam("p", "Multiplier to increase the number of partitions of feature datasets during joins", "PARALLEL", "8"),
      "row-bloomfilter-threshold" -> OptionParam("rbt", "Performance tuning, if observation record # is less than the threshold, a bloomfilter will be applied", "ROWFILTERTHRESHOLD", "-1"),
      "job-version" -> OptionParam("jv", "Job version, integer, job version uses DataFrame and SQL based anchor, default is 2", "JOBVERSION", "2"),
      "as-tensors" -> OptionParam("at", "If set to true, get features as tensors else as term-vectors", "AS_TENSORS", "false"),
      "redis-config" -> OptionParam("rc", "Authentication config for Redis", "REDIS_CONFIG", ""),
      "s3-config" -> OptionParam("sc", "Authentication config for S3", "S3_CONFIG", ""),
      "adls-config" -> OptionParam("adlc", "Authentication config for ADLS (abfs)", "ADLS_CONFIG", ""),
      "blob-config" -> OptionParam("bc", "Authentication config for Azure Blob Storage (wasb)", "BLOB_CONFIG", ""),
      "sql-config" -> OptionParam("sqlc", "Authentication config for Azure SQL Database (jdbc)", "SQL_CONFIG", ""),
      "snowflake-config" -> OptionParam("sfc", "Authentication config for Snowflake Database (jdbc)", "SNOWFLAKE_CONFIG", ""),
      "system-properties" -> OptionParam("sps", "Additional System Properties", "SYSTEM_PROPERTIES_CONFIG", "")
    )

    val extraOptions = List(new CmdOption("LOCALMODE", "local-mode", false, "Run in local mode"))

    val cmdParser = new CmdLineParser(args, params, extraOptions)

    // not used in PQO, set system properties passed via arguments
    val sps = cmdParser.extractOptionalValue("system-properties").getOrElse("{}")
    val props = (new ObjectMapper()).registerModule(DefaultScalaModule).readValue(sps, classOf[mutable.HashMap[String, String]])
    props.foreach(e => scala.util.Properties.setProp(e._1, e._2))

    // get join config argument using cmdParser
    val joinJobConfig = cmdParser.extractRequiredValue("join-config")

    val inputData = {
      val input = cmdParser.extractRequiredValue("input")
      val sourceType = SourceFormatType.withName(cmdParser.extractRequiredValue("source-type"))
      val startDate = cmdParser.extractOptionalValue("start-date")
      val endDate = cmdParser.extractOptionalValue("end-date")
      val numDays = cmdParser.extractOptionalValue("num-days")
      val dateOffset = cmdParser.extractOptionalValue("date-offset")

      InputData(input, sourceType, startDate, endDate, dateOffset, numDays)
    }

    // not used in PQO
    val passThroughFeatures = {
      cmdParser.extractRequiredValue("pass-through-features") match {
        case "" => Set.empty[String]
        case str => str.split(",") map (_.trim) toSet
      }
    }

    val feathrLocalConfig = cmdParser.extractOptionalValue("feathr-config")
    val feathrFeatureConfig = cmdParser.extractOptionalValue("feature-config")
    val localOverrideAll = cmdParser.extractRequiredValue("local-override-all")
    val outputPath = DataLocation(cmdParser.extractRequiredValue("output"))
    val numParts = cmdParser.extractRequiredValue("num-parts").toInt

    val joinJobContext = JoinJobContext(feathrLocalConfig, feathrFeatureConfig, Some(inputData), outputPath, numParts)

    // get other DataSource related arguments like redis-config, s3-config, adls-config,
    val dataSourceConfigs = DataSourceConfigUtils.getConfigs(cmdParser)

    // combine all the arguments together to form the FeathrJoinJobContext
    FeathrJoinJobContext(joinJobConfig, joinJobContext, dataSourceConfigs)
  }

  def run(sparkSession: SparkSession,
          hadoopConf: Configuration,
          feathrJoinJobContext: FeathrJoinJobContext,
          dataPathHandlers: List[DataPathHandler]): Unit = {
    val dataLoaderHandlers: List[DataLoaderHandler] = dataPathHandlers.map(_.dataLoaderHandler)

    // read the content from feature_join.conf
    val featureJoinConfig = FeatureJoinConfig.parseJoinConfig(
      hdfsFileReader(sparkSession, feathrJoinJobContext.joinJobConfig)
    )

    checkAuthorization(sparkSession, hadoopConf, feathrJoinJobContext, dataLoaderHandlers)

    feathrJoinRun(
      sparkSession = sparkSession,
      hadoopConf = hadoopConf,
      featureJoinConfig = featureJoinConfig,
      joinJobContext = feathrJoinJobContext.joinJobContext,
      localTestConfig = None,
      dataPathHandlers = dataPathHandlers
    )
  }

  private def checkAuthorization(sparkSession: SparkSession,
                                 hadoopConf: Configuration,
                                 feathrJoinJobContext: FeathrJoinJobContext,
                                 dataLoaderHandlers: List[DataLoaderHandler]): Unit = {
    feathrJoinJobContext.joinJobContext.outputPath match {
      case SimplePath(path) => {
        AclCheckUtils.checkWriteAuthorization(hadoopConf, path) match {
          case Failure(e) =>
            throw new FeathrDataOutputException(
              ErrorLabel.FEATHR_USER_ERROR,
              s"No write permission for output path ${feathrJoinJobContext.joinJobContext.outputPath}.",
              e
            )
          case Success(_) =>
            println("Checked write authorization on output path: " + feathrJoinJobContext.joinJobContext.outputPath)
        }
      }
      case _ => {}
    }

    feathrJoinJobContext.joinJobContext.inputData.foreach(inputData => {
      val failOnMissing = FeathrUtils.getFeathrJobParam(sparkSession, FeathrUtils.FAIL_ON_MISSING_PARTITION).toBoolean
      val pathList = SourceUtils.getPathList(
        sourceFormatType = inputData.sourceType,
        sourcePath = inputData.inputPath,
        ss = sparkSession,
        dateParam = inputData.dateParam,
        targetDate = None,
        failOnMissing = failOnMissing,
        dataLoaderHandlers = dataLoaderHandlers
      )
      AclCheckUtils.checkReadAuthorization(hadoopConf, pathList) match {
        case Failure(e) => throw new FeathrInputDataException(ErrorLabel.FEATHR_USER_ERROR, s"No read permission on observation data $pathList.", e)
        case Success(_) => println(s"Checked read authorization on observation data of the following paths: ${pathList.mkString("\n")}")
      }
    })
  }

  def hdfsFileReader(ss: SparkSession, path: String): String = {
    ss.sparkContext.textFile(path).collect.mkString("\n")
  }

  private[feathr] def feathrJoinRun(sparkSession: SparkSession,
                                    hadoopConf: Configuration,
                                    featureJoinConfig: FeatureJoinConfig,
                                    joinJobContext: JoinJobContext,
                                    dataPathHandlers: List[DataPathHandler],
                                    localTestConfig: Option[LocalTestConfig] = None
                                   ): (Option[RDD[GenericRecord]], Option[DataFrame]) = {

    val dataLoaderHandlers: List[DataLoaderHandler] = dataPathHandlers.map(_.dataLoaderHandler)
    val featureGroupings = featureJoinConfig.featureGroupings

    println("== featuresToTimeDelayMap ==")
    println(featureJoinConfig.featuresToTimeDelayMap)
    println()

    val failOnMissing = FeathrUtils.getFeathrJobParam(sparkSession, FeathrUtils.FAIL_ON_MISSING_PARTITION).toBoolean
    println("== Read Observation DataFrame ==")
    val observationDF =
      SourceUtils.loadObservationAsDF(
        sparkSession,
        hadoopConf,
        joinJobContext.inputData.get,
        dataLoaderHandlers,
        failOnMissing
      )

    //join the features using feathr client
    val (joinedDF, _) = getFeathrClientAndJoinFeatures(
      sparkSession,
      observationDF,
      featureGroupings,
      featureJoinConfig,
      joinJobContext,
      dataPathHandlers,
      localTestConfig
    )

    // val parameters = Map(SparkIOUtils.OUTPUT_PARALLELISM -> jobContext.numParts.toString, SparkIOUtils.OVERWRITE_MODE -> "ALL")
    // SparkIOUtils.writeDataFrame(joinedDF, jobContext.outputPath, parameters, dataLoaderHandlers)
    (None, Some(joinedDF))
  }

  private[offline] def getFeathrClientAndJoinFeatures(sparkSession: SparkSession,
                                                      observationDF: DataFrame,
                                                      featureGroupings: Map[String, Seq[JoiningFeatureParams]],
                                                      featureJoinConfig: FeatureJoinConfig,
                                                      joinJobContext: JoinJobContext,
                                                      dataPathHandlerList: List[DataPathHandler],
                                                      localTestConfigOpt: Option[LocalTestConfig] = None): (DataFrame, Header) = {

    val feathrClientPQO = getFeathrClient(sparkSession, joinJobContext, dataPathHandlerList, localTestConfigOpt)

    feathrClientPQO.dispatchFeathrJob(
      featureJoinConfig = featureJoinConfig,
      joinJobContext = joinJobContext,
      observationDF = observationDF
    )
  }

  private[offline] def getFeathrClient(sparkSession: SparkSession,
                                       joinJobContext: JoinJobContext,
                                       dataPathHandlers: List[DataPathHandler],
                                       localTestConfigOpt: Option[LocalTestConfig] = None): FeathrClientPQO = {
    localTestConfigOpt match {
      case None =>
        FeathrClientPQO.builder(sparkSession)
          .addFeatureDefPath(joinJobContext.feathrFeatureConfig)
          .addLocalOverrideDefPath(joinJobContext.feathrLocalConfig)
          .addDataPathHandlerList(dataPathHandlers)
          .build()
      case Some(localTestConfig) =>
        FeathrClientPQO.builder(sparkSession)
          .addFeatureDef(localTestConfig.featureConfig)
          .addLocalOverrideDef(localTestConfig.localConfig)
          .addDataPathHandlerList(dataPathHandlers)
          .build()
    }

  }

  case class FeathrJoinPreparationInfo(sparkSession: SparkSession,
                                       hadoopConf: Configuration,
                                       feathrJoinJobContext: FeathrJoinJobContext)

  case class FeathrJoinJobContext(joinJobConfig: String,
                                  joinJobContext: JoinJobContext,
                                  dataSourceConfigs: DataSourceConfigs)

  case class FeathrJoinJob(joinJobName: String,
                           joinTag: JobTagPQO)
}
