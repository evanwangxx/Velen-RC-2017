package com.sankuai.payrc.mlearn.spark.service.dataset

import com.sankuai.payrc.mlearn.spark.service.TaskExecutor
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

object RegisterDataFileExecutor extends TaskExecutor{
  override def execute(sc: SparkContext, sqlContext: HiveContext, config: Map[String, Any]): Map[String, Any] = {
    val path: String = config.get("path").get.asInstanceOf[String]
    val separator: String = config.get("separator").get.asInstanceOf[String]
    val charSet: String = config.get("charSet").get.asInstanceOf[String]

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("delimiter", separator)
      .option("charset", charSet)
      .load(path)

    var info = RegisterDataFile.inferDataTypes(df)

    info += ("path" -> path, "separator" -> separator, "charSet" -> charSet)

    return info
  }
}
