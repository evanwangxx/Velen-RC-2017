package com.sankuai.payrc.mlearn.spark.util

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object JsonHelper {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def toJson(value: Any): String = {
    mapper.writeValueAsString(value)
  }

  def toMap[V](json: String): Map[String, V] = {
    mapper.readValue(json, classOf[Map[String, V]])
  }
}
