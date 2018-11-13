package com.sankuai.payrc.mlearn.spark.service

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

trait TaskExecutor {
  def execute(sc: SparkContext, sqlContext: HiveContext, config: Map[String, Any]): Map[String, Any]
}
