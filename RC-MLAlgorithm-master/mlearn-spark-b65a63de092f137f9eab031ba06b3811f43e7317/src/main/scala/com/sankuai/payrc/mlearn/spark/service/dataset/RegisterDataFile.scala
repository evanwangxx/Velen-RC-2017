package com.sankuai.payrc.mlearn.spark.service.dataset

import com.sankuai.payrc.mlearn.spark.util.DataType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

object RegisterDataFile {
  def inferColumnDType(colNames: Array[String], dTypes: Array[Array[Int]], name: String): Int = {
    val colIndex: Int = colNames.indexOf(name)
    var dTypeCountsMap = Map[Int, Int]()

    dTypes.map(r => r(colIndex)).foreach(key => {
      if (key != DataType.NULL) {
        if (!dTypeCountsMap.contains(key)) {
          dTypeCountsMap += (key -> 0)
        }
        dTypeCountsMap += (key -> (dTypeCountsMap(key) + 1))
      }
    })

    var dType = DataType.NULL

    val dTypeCounts = dTypeCountsMap.toList

    val dTypeCountsSorted = dTypeCounts sortBy (-_._2)

    val threshold = 1000

    if (dTypeCountsSorted.size == 0) {
      return DataType.NULL
    } else if (dTypeCountsSorted.size == 1) {
      return dTypeCountsSorted(0)._1
    } else if (dTypeCountsSorted(0)._1 == DataType.BOOL) {
      return DataType.BOOL
    } else if (dTypeCountsSorted(0)._1 == DataType.STRING) {
      return DataType.STRING
    } else if (dTypeCountsSorted(0)._2 >= dTypeCountsSorted(1)._2 * threshold) {
      return dTypeCountsSorted(0)._1
    }

    return DataType.DOUBLE
  }

  def inferDataTypes(data: RDD[Array[String]], colNames: Array[String]): Map[String, Any] = {
    val lineCount = data.count()
    val inferDataTypeLineThreshold: Long = 10000;

    var sample = 1.0 * inferDataTypeLineThreshold / lineCount
    if (inferDataTypeLineThreshold >= lineCount) {
      sample = 1
    }

    val headLines: Array[Array[String]] = data.take(20)

    val dTypes = data.sample(false, sample).map((row: Array[String]) => colNames.map(
      name => DataType.inferDataType(row(colNames.indexOf(name))))).collect()

    val colDTypes = colNames.map(inferColumnDType(colNames, dTypes, _))

    return Map("lineCount" -> lineCount, "columns" -> colNames,
      "headLines" -> headLines, "columnDataTypes" -> colDTypes)
  }

  def anyToString(d: Any): String = {
    return if (d == null) null else d.toString
  }

  def inferDataTypes(df: DataFrame): Map[String, Any] = {
    val lineCount = df.count()
    val inferDataTypeLineThreshold: Long = 10000;

    var sample = 1.0 * inferDataTypeLineThreshold / lineCount
    if (inferDataTypeLineThreshold >= lineCount) {
      sample = 1
    }

    val colNames = df.columns

    val headLines: Array[Array[String]] = df.take(20).map((row: Row) => colNames.map(
      name => anyToString(row.get(row.fieldIndex(name)))))

    val sampled = df.sample(false, sample)

    val dTypes = sampled.map((row: Row) => colNames.map(
      name => DataType.inferDataType(row.get(row.fieldIndex(name))))).collect()

    val colDTypes = colNames.map(inferColumnDType(colNames, dTypes, _))

    return Map("lineCount" -> lineCount, "columns" -> colNames,
      "headLines" -> headLines, "columnDataTypes" -> colDTypes)
  }

//  def register(sc: SparkContext, sqlContext: HiveContext, config: Map[String, Any]): Map[String, Any] = {
//    val path: String = config.get("path").get.asInstanceOf[String]
//
//    val df = sqlContext.read.format("json").load(path)
//
//    df.
//
//    return null
//  }
}
