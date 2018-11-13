package com.sankuai.payrc.mlearn.spark.util

import com.sankuai.payrc.mlearn.spark.service.dataset.RegisterDataFile
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext

object csv {
  def load(sc: SparkContext, sqlContext: HiveContext, config: Map[String, Any]): DataFrame = {
    val path: String = config.get("path").get.asInstanceOf[String]
    val separator: String = config.get("separator").get.asInstanceOf[String]

    val rdd: RDD[Array[String]] = sc.textFile(path).map(_.split(separator))

    val r = RddHelper.splitHeader[String](rdd)

    val colNames: Array[String] = r._1
    val data: RDD[Array[String]] = r._2

    val info: Map[String, Any] = RegisterDataFile.inferDataTypes(data, colNames)
    val colDTypes = info.get("columnDataTypes").get.asInstanceOf[Array[Int]]
    val schema = DataType.schemaFromDataTypes(colNames, colDTypes)

    val dataRow: RDD[Row] = {
      data.map(arr => Row.fromSeq(arr.zip(colDTypes).map(aa => DataType.getValue(aa._1, aa._2))))
    }

    return sqlContext.createDataFrame(dataRow, schema)
  }
}
