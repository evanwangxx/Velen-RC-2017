package com.sankuai.payrc.mlearn.spark.service
import com.sankuai.payrc.mlearn.spark.service.dataset.RegisterDataFile
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

object TestExecutor extends TaskExecutor{
  override def execute(sc: SparkContext, sqlContext: HiveContext, config: Map[String, Any]): Map[String, Any] = {
    val sql = """SELECT partner, outmoney, dt
                |FROM ba_rc.web_rc_risklog
                |WHERE action IN (32, 33)
                |    AND dt = 20170701""".stripMargin
//
    val df = sqlContext.sql(sql)
    val path = "gaoyangbo02/mlearn/data/test.csv"
//
//    df.write
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .save(path)

//    val df = sqlContext.read
//      .format("com.databricks.spark.csv")
//      .option("header", "true") // Use first line of all files as header
//      .option("inferSchema", "true") // Automatically infer data types
//      .load(path)

    df.show(20)

    println(RegisterDataFile.inferDataTypes(df))

    return Map()
  }
}
