package com.linkedin.feathr.offline.pqo

import scala.beans.BeanProperty
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.collection.mutable.ListBuffer



object FeatureProvenanceDeserializer {

  private val jsonMapper = JsonMapper.builder().addModule(DefaultScalaModule).build()

  def deserializeProvenance(): ListBuffer[Provenance] = {
    println("== Deserialize Provenance Information")

    val jsonFileList = FeatureProvenanceUtils.readFeatureProvenance("/home/ruiliu/Develop/feathr-pqo/provenance")

    val jsonMapper = JsonMapper.builder().addModule(DefaultScalaModule).build()

    val provenanceList = new ListBuffer[Provenance]

    for (jsonFile <- jsonFileList) {
      if (jsonFile.toString == "/home/ruiliu/Develop/feathr-pqo/provenance/feature_provenance.json") {
        provenanceList += jsonMapper.readValue(jsonFile, classOf[Provenance])
      }
    }
    provenanceList
  }


  case class JoinFeature(@JsonProperty("joinKey") @BeanProperty joinKey: ListBuffer[String],
                         @JsonProperty("joinFeatureList") @BeanProperty joinFeatureList: ListBuffer[String])
  case class JobSetting(@JsonProperty("featureJoin") @BeanProperty featureJoin: ListBuffer[JoinFeature],
                        @JsonProperty("timeStampColumnDef") @BeanProperty timeStampColumnDef: String,
                        @JsonProperty("timeStampColumnFormat") @BeanProperty timeStampColumnFormat: String)

  case class FeatureItem(@JsonProperty("featureName") @BeanProperty featureName: String,
                         @JsonProperty("featureDef") @BeanProperty featureDef: String,
                         @JsonProperty("featureType") @BeanProperty featureType: String,
                         @JsonProperty("tensorCategory") @BeanProperty tensorCategory: String,
                         @JsonProperty("dimensionType") @BeanProperty dimensionType: ListBuffer[String],
                         @JsonProperty("valType") @BeanProperty valType: String,
                         @JsonProperty("source") @BeanProperty source: String,
                         @JsonProperty("key") @BeanProperty key: ListBuffer[String],
                         @JsonProperty("inputs") @BeanProperty inputs: ListBuffer[String],
                         @JsonProperty("aggregation") @BeanProperty aggregation: String,
                         @JsonProperty("aggWindow") @BeanProperty aggWindow: String)

  case class SourceItem(@JsonProperty("sourceName") @BeanProperty sourceName: String,
                        @JsonProperty("location") @BeanProperty location: String,
                        @JsonProperty("timestampColumn") @BeanProperty timestampColumn: String,
                        @JsonProperty("timestampColumnFormat") @BeanProperty timestampColumnFormat: String)

  case class ProvenanceItem(@JsonProperty("jobName") @BeanProperty jobName: String,
                            @JsonProperty("observation") @BeanProperty observation: String,
                            @JsonProperty("sources") @BeanProperty sources: ListBuffer[SourceItem],
                            @JsonProperty("features") @BeanProperty features: ListBuffer[FeatureItem],
                            @JsonProperty("joinSettings") @BeanProperty joinSettings: JobSetting)

  case class Provenance(@JsonProperty("provenance") @BeanProperty items: ListBuffer[ProvenanceItem])
}
