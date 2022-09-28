package com.linkedin.feathr.offline.pqo

import com.linkedin.feathr.offline.config.FeatureJoinConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

object MaterializedFeatureMapper {

  private val redisSparkSession = SparkSession
    .builder()
    .appName("online-store-redis")
    .master("local[4]")
    .config("spark.redis.host", "localhost")
    .config("spark.redis.port", "6379")
    .getOrCreate()

  def retrieveMaterializedFeatures(featureJoinConfig: FeatureJoinConfig,
                                   materializedFeatureName: String): DataFrame = {
    println("== Checking Materialized Features From Online Store ==")

    var materializedDF = redisSparkSession.read
      .format("org.apache.spark.sql.redis")
      .option("table", materializedFeatureName)
      .option("host", "localhost")
      .option("port", "6379")
      .option("auth", "123456")
      .load()

    if (materializedDF.isEmpty) {
      throw new RuntimeException("Cannot find the data frame in Redis")
    }

    materializedDF
  }

}
