package com.sankuai.payrc.mlearn.spark.service.modeltrain

import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.{DecisionTreeClassifier, LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.DataFrame

object PipelineBuilder {
  val ScaleFeatSuffix = "Scale_Mercury"
  val NormalizeFeatSuffix = "Normalize_Jupiter"
  val OneHotFeatSuffix = "OneHot_Venus"
  val MinMaxFeatSuffix = "MinMax_Mars"

  def featNamesModified(feats: Array[String],
                        normalize: Array[String],
                        oneHot: Array[String],
                        scale: Array[String],
                        minMax: Array[String]) : Array[String] = {

    var featNames = feats
      .filter(name => !oneHot.contains(name))
      .filter(name => !scale.contains(name))
      .filter(name => !minMax.contains(name))
      .filter(name => !normalize.contains(name))

    featNames = featNames ++ oneHot.map(name => name + "_" + OneHotFeatSuffix)

    if (!scale.isEmpty){
      featNames = featNames :+ ScaleFeatSuffix
    }

    if (!minMax.isEmpty){
      featNames = featNames :+ MinMaxFeatSuffix
    }

    if (!normalize.isEmpty) {
      featNames = featNames :+ NormalizeFeatSuffix
    }

    return featNames
  }

  def normalizePipeline(feats: Array[String], dataFrame: DataFrame): org.apache.spark.ml.PipelineStage = {
    val intName = "normalizeFeatureVector"
    val featVector = new VectorAssembler().setInputCols(feats).setOutputCol(intName)

    val pipelineStage: org.apache.spark.ml.PipelineStage = new StandardScaler()
      .setInputCol(intName)
      .setOutputCol(NormalizeFeatSuffix)
      .setWithStd(false)
      .setWithMean(true)

    return new Pipeline().setStages(Array(featVector, pipelineStage))
  }

  def scalePipeline(feats: Array[String], dataFrame: DataFrame): org.apache.spark.ml.PipelineStage = {
    val intName = "scaleFeatureVector"
    val featVector = new VectorAssembler().setInputCols(feats).setOutputCol(intName)

    val pipelineStage: org.apache.spark.ml.PipelineStage = new StandardScaler()
      .setInputCol(intName)
      .setOutputCol(ScaleFeatSuffix)
      .setWithStd(true)
      .setWithMean(true)

    return new Pipeline().setStages(Array(featVector, pipelineStage))
  }

  def oneHotPipeline(feats: Array[String], dataFrame: DataFrame): org.apache.spark.ml.PipelineStage = {

    val indexTransformers: Array[org.apache.spark.ml.PipelineStage] = feats.map(
      name => new StringIndexer()
        .setInputCol(name)
        .setOutputCol(s"${name}_" + OneHotFeatSuffix)
    )

    val indexColumns  = dataFrame.columns.filter(name => name contains "_" + OneHotFeatSuffix)

    val oneHotEncoders: Array[org.apache.spark.ml.PipelineStage] = indexColumns.map(
      cname => new OneHotEncoder()
        .setInputCol(cname)
        .setOutputCol(s"${cname}_" + OneHotFeatSuffix)
    )

    return new Pipeline()
      .setStages(indexTransformers ++ oneHotEncoders)
  }

  def minMaxPipeline(feats: Array[String], dataFrame: DataFrame): org.apache.spark.ml.PipelineStage = {
    val intName = "MinMaxFeatureVector"
    val featVector = new VectorAssembler().setInputCols(feats).setOutputCol(intName)

    val pipelineStage: org.apache.spark.ml.PipelineStage = new MinMaxScaler()
      .setInputCol(intName)
      .setOutputCol(MinMaxFeatSuffix)
      .setMax(1.0)
      .setMin(0.0)

    return new Pipeline().setStages(Array(featVector, pipelineStage))
  }

  def build(label: String, selectColumns: List[Map[String, Any]],
            model: String, df: DataFrame, parameters: Map[String, Any]): Pipeline = {

    val params: Map[String, Any] = if (parameters == null) Map() else parameters
    val feats = selectColumns.map(_.get("name").get.toString).toArray

    val normalize = selectColumns.filter(_.getOrElse("transform", "none").toString.equals("normalize"))
          .map(m => m.get("name").get.toString).toArray
    val oneHot = selectColumns.filter(_.getOrElse("transform", "none").toString.equals("onehot"))
          .map(m => m.get("name").get.toString).toArray
    val scale = selectColumns.filter(_.getOrElse("transform", "none").toString.equals("scale"))
          .map(m => m.get("name").get.toString).toArray
    val minMax = selectColumns.filter(_.getOrElse("transform", "none").toString.equals("minmax"))
          .map(m => m.get("name").get.toString).toArray

    var stages: Array[PipelineStage] = Array()

    if (!normalize.isEmpty) {
      stages = stages :+ normalizePipeline(normalize, df)
    }

    if (!scale.isEmpty) {
      stages = stages :+ scalePipeline(scale, df)
    }

    if (!oneHot.isEmpty) {
      stages = stages :+ oneHotPipeline(scale, df)
    }

    if (!minMax.isEmpty) {
      stages = stages :+ minMaxPipeline(scale, df)
    }

    val labelColName = "labelIndexed"
    val labelIndexer = new StringIndexer().setInputCol(label).setOutputCol(labelColName).fit(df)
    stages = stages :+ labelIndexer

    val featNamesAfterModify = featNamesModified(feats, normalize, oneHot, scale, minMax)
    val iputColName = "featureVector"
    val inputVectorAssembler = new VectorAssembler().setInputCols(featNamesAfterModify).setOutputCol(iputColName)

    stages = stages :+ inputVectorAssembler

    val classifier: PipelineStage = model match {
      case "rf" => new RandomForestClassifier().setLabelCol(labelColName).setFeaturesCol(iputColName)
        .setMaxDepth(params.getOrElse("maxDepth", 7).toString.toInt)
        .setImpurity(params.getOrElse("impurity", "gini").toString)
        .setMinInfoGain(params.getOrElse("minInfoGain", 0).toString.toDouble)
        .setNumTrees(params.getOrElse("numTree", 100).toString.toInt)
        .setFeatureSubsetStrategy("auto");
      case "lr" => new LogisticRegression().setLabelCol(labelColName).setFeaturesCol(iputColName)
        .setMaxIter(params.getOrElse("maxIteration", 1000).toString.toInt)
        .setRegParam(params.getOrElse("regParameter", 0.1).toString.toDouble)
      case _ => throw new Exception("无法识别的模型: " + model)
    }

    stages = stages :+ classifier

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("prediction_Labeled")
      .setLabels(labelIndexer.labels)

    stages = stages :+ labelConverter

    val pipeline = new Pipeline().setStages(stages)
    return pipeline
  }

}
