package com.sankuai.payrc.mlearn.spark.util

import org.apache.spark.sql.types._

object DataType {
  val NULL: Int = 0
  val BOOL: Int = 1
  val INT: Int = 2
  val LONG: Int = 3
  val FLOAT: Int = 4
  val DOUBLE: Int = 5
  val STRING: Int = 6

  def inferDataType(valueAny: Any, doubleFirst: Boolean=true): Int = {
    if (null == valueAny) {
      return NULL
    }

    val value = valueAny.toString.trim.toLowerCase
    if (value == "" || value == "null" || value == "none") {
      return NULL
    }

    if (value == "true" || value == "false") {
      return BOOL
    }

    try {
      value.toInt
      return INT
    } catch {
      case e: Exception => {
      }
    }

    try {
      value.toLong
      return LONG
    } catch {
      case e: Exception => {
      }
    }

    try {
      value.toFloat
      return FLOAT
    } catch {
      case e: Exception => {
      }
    }

    try {
      value.toDouble
      return DOUBLE
    } catch {
      case e: Exception => {
      }
    }

    return STRING
  }

  def getValue(value: String, dType: Int): Any = {
    if (value == null) {
      return null
    }
    try {
      return dType match {
        case NULL => null;
        case BOOL => if (value.toLowerCase == "true") true else false;
        case INT => value.trim.toInt;
        case LONG => value.trim.toLong;
        case FLOAT => value.trim.toFloat;
        case DOUBLE => value.trim.toDouble;
        case STRING => value;
      }
    } catch {
      case e: Exception => return null
    }
  }

  def schemaFromDType(name: String, dType: Int): StructField = {
    val fieldType = dType match {
      case BOOL => BooleanType;
      case INT => IntegerType;
      case LONG => LongType;
      case FLOAT => FloatType;
      case DOUBLE => DoubleType;
      case _ => StringType;
    }

    return new StructField(name, fieldType)
  }

  def schemaFromDataTypes(names: Array[String], dTypes: Array[Int]): StructType = {
    return new StructType(names.map(name => schemaFromDType(name, dTypes(names.indexOf(name)))))
  }
}
