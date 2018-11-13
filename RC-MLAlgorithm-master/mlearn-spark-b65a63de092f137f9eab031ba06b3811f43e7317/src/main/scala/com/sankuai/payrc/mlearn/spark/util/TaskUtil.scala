package com.sankuai.payrc.mlearn.spark.util

import org.apache.spark.SparkContext

object TaskUtil {
  val StartMark: String = "$$TASK_START$$"
  val ResultMark: String = "$$RESULT$$"
  val EndMark: String = "$$TASK_END$$"

  val SPARK_TASK_CONFIGS: String = "wanghongbo05/mlearn/spark-task-configs/"

  def printResult(result: String): Unit = {
    println(ResultMark + " " + result)
  }

  def printStart(): Unit = {
    println(StartMark)
  }

  def printEnd(): Unit = {
    println(EndMark)
  }

  def parseArgs(sc: SparkContext, args: Array[String]): (String, Map[String, Any]) = {
    if (args.length < 1) {
      throw new Exception("Hope 任务参数错误。")
    }

    val argsA: Array[String] = if (args.length == 1) args(0).split(" ") else args

    val taskType: String = argsA(0).trim
    var config: Map[String, Any] = Map()

    if (argsA.length > 1) {
      val text = sc.textFile(SPARK_TASK_CONFIGS + argsA(1)).collect().reduce((a, b) => a + b)

      config = JsonHelper.toMap[Any](text)
    }

    return (taskType, config)
  }
}
