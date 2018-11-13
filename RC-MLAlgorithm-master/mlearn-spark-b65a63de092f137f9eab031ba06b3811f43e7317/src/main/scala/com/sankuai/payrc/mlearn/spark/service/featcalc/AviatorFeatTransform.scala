package com.sankuai.payrc.mlearn.spark.service.featcalc

import com.sankuai.payrc.mlearn.spark.service.TaskExecutor
import com.sankuai.payrc.mlearn.spark.service.dataset.RegisterDataFile
import com.sankuai.payrc.mlearn.spark.util.{AviatorHelper, DataType, JsonHelper, csv}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}

object AviatorFeatTransform extends TaskExecutor {
  def transform_df(df: DataFrame, feats: Array[Map[String, Any]]): RDD[Map[String, Any]] = {
    val colNames = df.columns
    val mapRdd = df.rdd.map((row: Row) => colNames.map((name: String) => (name, row.get(row.fieldIndex(name)))).toMap)

    val featsStr = JsonHelper.toJson(feats)
    val tt = mapRdd.map(s => AviatorHelper.batchExecute(featsStr, JsonHelper.toJson(s)))
    return tt.map(s => JsonHelper.toMap[Any](s))
  }

  override def execute(sc: SparkContext, sqlContext: HiveContext, config: Map[String, Any]): Map[String, Any] = {
    val path: String = config.get("path").get.asInstanceOf[String]
    val separator: String = config.get("separator").get.asInstanceOf[String]
    val charSet: String = config.get("charSet").get.asInstanceOf[String]
    val targetDataFile: String = config.get("targetDataFile").get.asInstanceOf[String]

    val targets: Array[String] = config.get("targets").get.asInstanceOf[List[String]].toArray

    val feats: Array[Map[String, Any]] = config.get("feats").get.asInstanceOf[List[Map[String, Any]]].toArray

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("delimiter", separator)
      .option("charset", charSet)
      .load(path)

    val rdd = transform_df(df, feats).map(m => targets.map(
          name => if (m.contains(name)) m.get(name).get.toString else null))

    val registerInfo: Map[String, Any] = RegisterDataFile.inferDataTypes(rdd, targets)
    val colDTypes = registerInfo.get("columnDataTypes").get.asInstanceOf[Array[Int]]
    val schema = DataType.schemaFromDataTypes(targets, colDTypes)

    val dataRow: RDD[Row] = {
      rdd.map(arr => Row.fromSeq(arr.zip(colDTypes).map(aa => DataType.getValue(aa._1, aa._2))))
    }

    val df_r = sqlContext.createDataFrame(dataRow, schema)

    df_r.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", separator)
      .option("charset", charSet)
      .save(targetDataFile)

    var info: Map[String, Any] = Map("path" -> targetDataFile, "separator" -> separator, "charSet" -> charSet)

    info = info ++ registerInfo

    return info
  }
}
