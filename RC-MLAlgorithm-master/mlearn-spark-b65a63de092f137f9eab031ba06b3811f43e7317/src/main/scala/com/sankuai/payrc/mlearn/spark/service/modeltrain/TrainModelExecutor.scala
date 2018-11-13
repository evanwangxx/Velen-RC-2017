package com.sankuai.payrc.mlearn.spark.service.modeltrain

import javax.xml.transform.stream.StreamResult

import com.sankuai.payrc.mlearn.spark.service.TaskExecutor
import com.sankuai.payrc.mlearn.spark.util.DataType
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.jpmml.model.JAXBUtil
import org.jpmml.sparkml

object TrainModelExecutor extends TaskExecutor{
  def readDataFrameFromCsv(sqlContext: HiveContext, config: Map[String, Any]): DataFrame = {
    val path: String = config.get("path").get.asInstanceOf[String]
    val separator: String = config.get("separator").get.asInstanceOf[String]
    val charSet: String = config.get("charSet").get.asInstanceOf[String]

    val selectColumns = config.get("selectColumns").get.asInstanceOf[List[Map[String, Any]]]

    val featNames = selectColumns.map(col => col.get("name").get.toString)
    val featDTypes = selectColumns.map(col => col.get("dataType").get.toString.toInt)
    val schema = DataType.schemaFromDataTypes(featNames.toArray, featDTypes.toArray)

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("delimiter", separator)
      .option("charset", charSet)
      .load(path)

    val rdd = df.rdd.map((row: Row) => featNames.map((name: String) => row.get(row.fieldIndex(name)).toString))

    val dataRow: RDD[Row] = {
      rdd.map(arr => Row.fromSeq(arr.zip(featDTypes).map(aa => DataType.getValue(aa._1, aa._2))))
    }

    return sqlContext.createDataFrame(dataRow, schema)
  }

  override def execute(sc: SparkContext, sqlContext: HiveContext, config: Map[String, Any]): Map[String, Any] = {
    val selectColumns = config.get("selectColumns").get.asInstanceOf[List[Map[String, Any]]]
    val parameters = config.get("parameters").get.asInstanceOf[Map[String, Any]]
    val model = config.get("model").get.toString
    val label = config.get("y").get.toString

    val df = readDataFrameFromCsv(sqlContext, config)
    val rawSchema = df.schema

    val modelPath = config.get("modelPath").get.toString
    val pmmlPath = config.getOrElse("pmmlPath", "").toString


    val pipeline = PipelineBuilder.build(label, selectColumns, model, df, parameters)

    val modelTrained = pipeline.fit(df)

    val prediction = modelTrained.transform(df)

    prediction.select(label, "prediction_Labeled", "prediction").show(100)

//    val pmml = sparkml.ConverterUtil.toPMML(rawSchema, modelTrained)
//
//    JAXBUtil.marshalPMML(pmml, new StreamResult(System.out))

    return Map()
  }
}
