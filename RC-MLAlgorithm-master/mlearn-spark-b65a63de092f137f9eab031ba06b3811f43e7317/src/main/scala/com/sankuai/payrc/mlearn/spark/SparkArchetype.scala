package com.sankuai.payrc.mlearn.spark

import com.sankuai.payrc.mlearn.spark.service.dataset.RegisterDataFileExecutor
import com.sankuai.payrc.mlearn.spark.service.featcalc.AviatorFeatTransform
import com.sankuai.payrc.mlearn.spark.service.modeltrain.TrainModelExecutor
import com.sankuai.payrc.mlearn.spark.service.query.QueryTaskExecutor
import com.sankuai.payrc.mlearn.spark.service.{TaskExecutor, TestExecutor}
import com.sankuai.payrc.mlearn.spark.util.{JsonHelper, TaskUtil}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object SparkArchetype {

  val TaskExecutors: Map[String, TaskExecutor] = Map(
            "test" -> TestExecutor,
            "query" -> QueryTaskExecutor,
            "registerData" -> RegisterDataFileExecutor,
            "transformFeat" -> AviatorFeatTransform,
            "trainModel" -> TrainModelExecutor)

  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val sqlContext = new HiveContext(sc)

    sc.setLogLevel("ERROR")

    TaskUtil.printStart()

    var result: Map[String, Any] = Map()

    try {
      val taskArgs = TaskUtil.parseArgs(sc, args)
      val taskType: String = taskArgs._1
      val config: Map[String, Any] = taskArgs._2

      println("运行 %s 任务, 配置: %s".format(taskType, config))

      if (!TaskExecutors.contains(taskType)) {
        throw new Exception("任务类型%s不存在。".format(taskType))
      }

      val data = TaskExecutors.get(taskType).get.execute(sc, sqlContext, config)

      result += ("data" -> data)
      result += ("status" -> 0)
    } catch {
      case e: Exception => {
        result += ("status" -> 1)
        result += ("msg" -> e.getMessage)
        result += ("stackTrace" -> e.getStackTraceString)
      }
    }

    TaskUtil.printResult(JsonHelper.toJson(result))

    TaskUtil.printEnd()

    sc.stop()
  }
}
