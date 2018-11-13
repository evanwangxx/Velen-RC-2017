package com.sankuai.payrc.mlearn.spark.util

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object RddHelper {
  def splitHeader[T:ClassTag](rdd: RDD[Array[T]]): (Array[T], RDD[Array[T]]) = {
    val colNames = rdd.first()

    val data = rdd.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    return (colNames, data)
  }
}
