package com.sankuai.payrc.mlearn.spark.service.query

import java.util.UUID

import com.sankuai.payrc.mlearn.spark.service.TaskExecutor
import com.sankuai.payrc.mlearn.spark.service.dataset.RegisterDataFile
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

object QueryTaskExecutor extends TaskExecutor{
  val SPARK_DATA = "wanghongbo05/mlearn/data/"

  override def execute(sc: SparkContext, sqlContext: HiveContext, config: Map[String, Any]): Map[String, Any] = {
    val sql: String = config.get("sql").get.asInstanceOf[String]
    val register: Boolean = config.get("register").get.asInstanceOf[Boolean]
    val name: String = UUID.randomUUID().toString
    val path = SPARK_DATA + name + ".csv"

    val df = sqlContext.sql(sql)

    var info: Map[String, Any] = Map("name" -> name, "path" -> path)

    if (register) {
      val registerInfo = RegisterDataFile.inferDataTypes(df)
      info = info ++ registerInfo
    }

    val separator = "\t"
    val charSet = "UTF-8"

    df.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", separator)
      .option("charset", charSet)
      .save(path)

    info += ("path" -> path, "separator" -> separator, "charSet" -> charSet)

    return info
  }
}
