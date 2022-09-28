package com.linkedin.feathr.offline.pqo

import com.linkedin.feathr.common
import com.linkedin.feathr.common.exception.{ErrorLabel, FeathrConfigException, FeathrException}
import com.linkedin.feathr.common.{FeatureDependencyGraph, JoiningFeatureParams}
import com.linkedin.feathr.offline.{ErasedEntityTaggedFeature, FeatureName, JoinStage, KeyTagIdTuple}
import com.linkedin.feathr.offline.anchored.feature.FeatureAnchorWithSource
import com.linkedin.feathr.offline.derived.DerivedFeature
import com.linkedin.feathr.offline.logical.{FeatureGroups, MultiStageJoinPlan}

import scala.collection.JavaConverters._
import scala.collection.convert.wrapAll._
import scala.collection.mutable


private[offline] class MultiStageJoinPlannerPQO extends LogicalPlannerPQO[MultiStageJoinPlanPQO] {

  def getLogicalPlan(featureGroups: FeatureGroups, keyTaggedFeatures: Seq[JoiningFeatureParams]): MultiStageJoinPlan = {
    println ("Get Original Logical Plan")

    val (allRequestedFeatures, keyTagIntsToStrings) = convertKeyTagsToIntegerIndexes (keyTaggedFeatures)

    val allRequiredFeatures = getDependencyOrdering (
      featureGroups.allAnchoredFeatures, featureGroups.allDerivedFeatures, allRequestedFeatures
    )

    val (windowAggFeatureStages, joinStages, postJoinDerivedFeatures) = getJoinStages(featureGroups, allRequiredFeatures)

    // split all the features to win-agg and non-win-agg feature
    val (requiredWindowAggFeatures, requiredNonWindowAggFeatures) = allRequestedFeatures.partition (
      taggedFeatureName => featureGroups.allWindowAggFeatures.contains (taggedFeatureName.getFeatureName)
    )

    // Extract SeqJoin features here; SeqJoin features will be specifically processed during the join
    // it is not used in some cases
    val (requestedSeqJoinFeatures, _) = allRequestedFeatures.partition (
    f => featureGroups.allSeqJoinFeatures.contains (f.getFeatureName)
    )

    MultiStageJoinPlan (
      windowAggFeatureStages,
      joinStages,
      postJoinDerivedFeatures,
      requiredWindowAggFeatures,
      requiredNonWindowAggFeatures,
      requestedSeqJoinFeatures,
      keyTagIntsToStrings,
      allRequiredFeatures,
      allRequestedFeatures
    )
  }

  def getLogicalPlanPQO(featureGroups: FeatureGroups,
                        keyTaggedFeatures: Seq[JoiningFeatureParams]): MultiStageJoinPlanPQO = {
    println("Get Provenance-based Optimization Logical Plan")

    val (allRequestedFeatures, keyTagIntsToStrings) = convertKeyTagsToIntegerIndexes(keyTaggedFeatures)

    val allRequiredFeatures = getDependencyOrdering(
      featureGroups.allAnchoredFeatures, featureGroups.allDerivedFeatures, allRequestedFeatures
    )

    val (windowAggFeatureStages, joinStages, postJoinDerivedFeatures) = getJoinStages(featureGroups, allRequiredFeatures)

    // split all the features to win-agg and non-win-agg feature
    val (requiredWindowAggFeatures, requiredNonWindowAggFeatures) = allRequestedFeatures.partition(
      taggedFeatureName => featureGroups.allWindowAggFeatures.contains(taggedFeatureName.getFeatureName)
    )

    // Extract SeqJoin features here; SeqJoin features will be specifically processed during the join
    // it is not used in some cases
    val (requestedSeqJoinFeatures, _) = allRequestedFeatures.partition(
      f => featureGroups.allSeqJoinFeatures.contains(f.getFeatureName)
    )

    println("XX requestedSeqJoinFeatures XX")
    println(requestedSeqJoinFeatures)
    println()

    MultiStageJoinPlanPQO(
      windowAggFeatureStages,
      joinStages,
      postJoinDerivedFeatures,
      requiredWindowAggFeatures,
      requiredNonWindowAggFeatures,
      requestedSeqJoinFeatures,
      keyTagIntsToStrings,
      allRequiredFeatures,
      allRequestedFeatures
    )
  }

  def getJoinStages(featureGroups: FeatureGroups,
                    requiredFeatures: Seq[common.ErasedEntityTaggedFeature]): (Seq[JoinStage], Seq[JoinStage], Seq[common.ErasedEntityTaggedFeature]) = {
    val allWindowAggFeatures = featureGroups.allWindowAggFeatures
    val allAnchoredFeatures = featureGroups.allAnchoredFeatures
    val allPassthroughFeatures = featureGroups.allPassthroughFeatures
    val allDerivedFeatures = featureGroups.allDerivedFeatures

    // get window aggregation feature
    val windowAggFeaturesOrdered = requiredFeatures.filter(taggedFeature => allWindowAggFeatures.contains(taggedFeature.getFeatureName))

    val requiredBasicAnchoredFeatures = requiredFeatures
      .filter(_.getBinding.nonEmpty) // Filter out only sequential join dependent expansion feature
      .filter(
        taggedFeature =>
          allAnchoredFeatures.contains(taggedFeature.getFeatureName)
            && (!allWindowAggFeatures.contains(taggedFeature.getFeatureName))
            && (!allPassthroughFeatures.contains(taggedFeature.getFeatureName))
      )

    println("== Basic Anchored Features ==")
    println(requiredBasicAnchoredFeatures)

    val derivedFeaturesOrdered = requiredFeatures.filter(taggedFeature => allDerivedFeatures.contains(taggedFeature.getFeatureName))
    println("== Derived Features ==")
    println(derivedFeaturesOrdered)

    // Window Aggregration
    // JoinStage: (KeyTagIdTuple, Seq[FeatureName])
    val windowAggFeatureStage: Seq[JoinStage] = windowAggFeaturesOrdered
      .groupBy(_.getBinding.map(_.toInt).toSeq).mapValues(_.map(_.getFeatureName).toIndexedSeq).toSeq

    println("XX windowAggFeatureStage XX")
    println(windowAggFeatureStage)
    println()

    // create a new hashmap for joinstage
    val joinStages = new mutable.HashMap[KeyTagIdTuple, Seq[FeatureName]]

    // add anchored features to join stage
    joinStages ++= requiredBasicAnchoredFeatures
      .groupBy(_.getBinding.map(_.asInstanceOf[Int]))
      .mapValues(_.map(_.getFeatureName).toIndexedSeq)

    println("== Join Stages After Adding Anchored Features ==")
    println(joinStages)
    println()

    val postJoinDerivedFeatures = new mutable.ArrayBuffer[common.ErasedEntityTaggedFeature]

    // add derived features to join stage
    derivedFeaturesOrdered.foreach(taggedFeatureName => {
      val ErasedEntityTaggedFeature(keyTag, featureRefStr) = taggedFeatureName
      joinStages.get(keyTag) match {
        case Some(featureNamesJoinedInThisStage) =>
          val dependencies = getDependenciesForTaggedFeature(allAnchoredFeatures, allDerivedFeatures, taggedFeatureName)
          if (dependencies.forall(x =>
            keyTag.equals(x.getBinding.map(_.toInt))
              && featureNamesJoinedInThisStage.contains(x.getFeatureName))) {
            joinStages.put(keyTag, joinStages.getOrElse(keyTag, Nil) :+ featureRefStr)
          } else {
            postJoinDerivedFeatures.append(taggedFeatureName)
          }
        case None =>
          postJoinDerivedFeatures.append(taggedFeatureName)
      }
    })

    println("== Join Stages After Adding Anchored and Derived Features ==")
    println(joinStages)
    println()

    // run join stages with fewer features first (in the hashmap of joinstages, sort by the size of value)
    // This should reduce the shuffle write size in some cases.

    val adjustedJoinStages = joinStages.mapValues(_.toIndexedSeq).toIndexedSeq.filter(_._1.nonEmpty).sortBy(_._2.size)
    println("== Join Stages According to Feature Size ==")
    println(adjustedJoinStages)
    println()

    (windowAggFeatureStage, adjustedJoinStages, postJoinDerivedFeatures)
  }


  def getDependencyOrdering(allAnchoredFeatures: Map[String, FeatureAnchorWithSource],
                            allDerivedFeatures: Map[String, DerivedFeature],
                            requestedFeatures: Seq[common.ErasedEntityTaggedFeature]): Seq[common.ErasedEntityTaggedFeature] = {

    val ordering = constructFeatureDependencyGraph(allDerivedFeatures, allAnchoredFeatures)
      .getPlan(requestedFeatures.map(_.getFeatureName).distinct).toIndexedSeq

    println("== Ordering of Dependency Features Map ==")
    println(ordering)

    // create a multiMap on top of a regular hashmap
    val scratch = new mutable.HashMap[String, mutable.Set[Seq[Int]]] with mutable.MultiMap[String, Seq[Int]]
    requestedFeatures.foreach { case ErasedEntityTaggedFeature(keyTag, featureRefStr) => scratch.addBinding(featureRefStr, keyTag) }

    ordering.reverseIterator.foreach(featureRefStr => {
      if(!scratch.contains(featureRefStr)) {
        throw new FeathrException(ErrorLabel.FEATHR_USER_ERROR, s"Unknown feature $featureRefStr in an internal data structure.")
      }

      val rawDependencies = getDependenciesForFeature(allAnchoredFeatures, allDerivedFeatures, featureRefStr)
      val callerKeyTags = scratch(featureRefStr)

      if (callerKeyTags.nonEmpty) {
        for (callerKeyTag <- callerKeyTags; ErasedEntityTaggedFeature(calleeKeyTag, dependencyName) <- rawDependencies) {
          if (calleeKeyTag.isEmpty) {
            scratch.addBinding(dependencyName, calleeKeyTag)
          } else {
            scratch.addBinding(dependencyName, calleeKeyTag.map(callerKeyTag))
          }
        }
      }
    })

    println("== Feature Map after going through all features ==")
    println(scratch)

    val ordering_seq = ordering.flatMap(x => scratch(x).map(ErasedEntityTaggedFeature(_, x)))
    println("== Ordering Seq == ")
    println(ordering_seq)
    ordering_seq
  }

  def getDependenciesForFeature(allAnchoredFeatures: Map[String, FeatureAnchorWithSource],
                                allDerivedFeatures: Map[String, DerivedFeature],
                                featureName: String): Seq[common.ErasedEntityTaggedFeature] = {
    if (allAnchoredFeatures.contains(featureName)) {
      Seq.empty[common.ErasedEntityTaggedFeature]
    } else if (allDerivedFeatures.contains(featureName)) {
      allDerivedFeatures(featureName).consumedFeatureNames
    } else {
      throw new FeathrConfigException(ErrorLabel.FEATHR_USER_ERROR, s"Feature $featureName is not defined in the config.")
    }
  }

  private def constructFeatureDependencyGraph(derivedFeatures: Map[String, DerivedFeature],
                                              anchoredFeatures: Map[String, FeatureAnchorWithSource]): FeatureDependencyGraph = {
    val dependencyFeaturesMap = derivedFeatures.map {
      case (featureName, derivedFeature) =>
        (featureName, derivedFeature.consumedFeatureNames.toSet.asJava)
    }.asJava

    println("== Dependency Features Map [Derived Features] ==")
    println(dependencyFeaturesMap)

    val anchoredFeaturesSet = anchoredFeatures.keySet.asJava
    new FeatureDependencyGraph(dependencyFeaturesMap, anchoredFeaturesSet)
  }

  def convertKeyTagsToIntegerIndexes(requestFeatures: Seq[JoiningFeatureParams]): (Seq[common.ErasedEntityTaggedFeature], Seq[String]) = {
    // get (primary) key identified in the config file
    val allKeyTagStrings = requestFeatures.flatMap(joiningFeatureParams => joiningFeatureParams.keyTags).distinct.toIndexedSeq
    println(s"Keys: ${allKeyTagStrings}")

    // get the [key -> index]
    val keyTagStringIndex: Map[String, Int] = allKeyTagStrings.zipWithIndex.toMap
    println(s"Keys with Index: ${keyTagStringIndex}")

    println("== Tagged Feature Names ==")
    for(e <- requestFeatures) println(e)

    // get a list of JoiningFeatureParams
    // a JoiningFeatureParams is a class consists params: keyTags, featureName, datePraram, timeDelay, featureAlias
    val taggedFeatureNames = requestFeatures.map {
      joiningFeatureParams =>
        if (joiningFeatureParams.featureAlias.isDefined && joiningFeatureParams.timeDelay.isDefined) {
          ErasedEntityTaggedFeature(joiningFeatureParams.keyTags.map(keyTagStringIndex).toIndexedSeq, joiningFeatureParams.featureAlias.get)
        } else {
          ErasedEntityTaggedFeature(joiningFeatureParams.keyTags.map(keyTagStringIndex).toIndexedSeq, joiningFeatureParams.featureName)
        }
    }.toIndexedSeq

    // show the feature parameters after erasing useless parameters
    println("== Erased Entity Tagged Feature ==")
    for(e <- taggedFeatureNames) println(e)

    (taggedFeatureNames, allKeyTagStrings)
  }

  private def getDependenciesForTaggedFeature(allAnchoredFeatures: Map[String, FeatureAnchorWithSource],
                                              allDerivedFeatures: Map[String, DerivedFeature],
                                              erasedEntityTaggedFeature: common.ErasedEntityTaggedFeature): Seq[common.ErasedEntityTaggedFeature] = {
    // ErasedEntityTaggedFeature.getBinding returns a java list of integers, need to convert to Seq[Int]
    getDependenciesForFeature(allAnchoredFeatures, allDerivedFeatures, erasedEntityTaggedFeature.getFeatureName).map {
      case ErasedEntityTaggedFeature(calleeKeyTag, dependencyName) =>
        ErasedEntityTaggedFeature(calleeKeyTag.map(erasedEntityTaggedFeature.getBinding.map(_.asInstanceOf[Int])), dependencyName)
    }
  }

}



private[offline] object MultiStageJoinPlannerPQO {

  // apply method is invoked by MultiStageJoinPlannerPQO()
  def apply(): MultiStageJoinPlannerPQO = {
    new MultiStageJoinPlannerPQO
  }
}
