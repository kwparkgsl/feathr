package com.linkedin.feathr.offline.pqo

import com.linkedin.feathr.offline.config.FeatureJoinConfig
import com.linkedin.feathr.offline.pqo.FeatureProvenanceDeserializer.Provenance

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object PQOArbitrator {
  def jobArbitrate(currentJobConfig: FeatureJoinConfig, provenanceList: ListBuffer[Provenance]): JobTagPQO.JobTagPQO ={
    val joinFeatureParamList = currentJobConfig.featureGroupings.values.flatten.toSeq.distinct
    val joinKeyNameSet = joinFeatureParamList.flatMap(joinFeatureParam => joinFeatureParam.keyTags).distinct.toSet
    val joinFeatureNameSet = joinFeatureParamList.map(joinFeaturePrarms => joinFeaturePrarms.featureName).toSet

    val provenanceKeyNameSet = mutable.Set[String]()
    val provenanceFeatureNameSet = mutable.Set[String]()

    for(provenance <- provenanceList) {
      val provIter = provenance.getItems.iterator
      while(provIter.hasNext) {
        val featureIter = provIter.next().getFeatures.iterator
        while(featureIter.hasNext) {
          val feature = featureIter.next()
          for(e <- feature.key) {
            provenanceKeyNameSet.add(e)
          }
          provenanceFeatureNameSet.add(feature.featureName)
        }
      }
    }

    if(joinKeyNameSet == provenanceKeyNameSet && joinFeatureNameSet == provenanceFeatureNameSet) {
      JobTagPQO.SAME
    }
    else if(joinFeatureNameSet.intersect(provenanceFeatureNameSet) == null) {
      JobTagPQO.NEW
    }
    else {
      JobTagPQO.SIMILAR
    }
  }
}
