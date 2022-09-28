package com.linkedin.feathr.offline.pqo

object JobTagPQO extends Enumeration {
  type JobTagPQO = Value
  val SAME, SAME_BUT_TIME, SIMILAR, NEW = Value
}
