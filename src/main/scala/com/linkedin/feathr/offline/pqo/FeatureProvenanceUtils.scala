package com.linkedin.feathr.offline.pqo

import java.io.File

object FeatureProvenanceUtils {

  def readFeatureProvenance(featureProvenanceFolder: String): List[File] = {
    val jsonFileList = getListOfFiles(featureProvenanceFolder)
    jsonFileList
  }

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }
}
