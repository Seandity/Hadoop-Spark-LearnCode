package com.leishen.project.spark.sql

import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * leishen on 2016/11/3.
  */
object FilterConditions {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("FilterConditions").setMaster("local[3]")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    val csvData = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", true.toString)
      .load("D:\\helloworld.csv")

    csvData.show()

    val columnFilter = missingAttributesFilter(csvData)

    csvData.filter(columnFilter).show()

    val column = customFilter(("name","startsWith","y"),csvData)

    csvData.except(csvData.filter(column)).show()
    println(csvData.schema("value").dataType)

    csvData.filter(csvData("value") between("1","4")).show()
    sparkContext.stop()
  }

  private def missingAttributesFilter(input : DataFrame): Column = {

    val columns = input.columns
    var opColumn = (input(columns(0)) isNull)

    for (i <- 1 to columns.length - 1) {
      val attributeColumn = (input(columns(i)) isNull)
        opColumn = opColumn or attributeColumn
    }
    opColumn
  }

  private def customFilter(opValue: (String,String,String), input: DataFrame): Column = {
    val attributeName = opValue._1
    val operate = opValue._2
    val targetValue = opValue._3

    operate match {
      case "=" =>
        input(attributeName) === targetValue

      case "!=" =>
        input(attributeName) !== targetValue
      case ">=" =>
        input(attributeName) >= targetValue

      case "<=" =>
        input(attributeName) <= targetValue

      case ">" =>
        input(attributeName) > targetValue

      case "<" =>
        input(attributeName) < targetValue

      case "endsWith" =>
        input(attributeName) endsWith (targetValue)

      case "isNotNull" =>
        input(attributeName) isNotNull

      case "isNull" =>
        input(attributeName) isNull

      case "startsWith" =>
        input(attributeName) startsWith (targetValue)

      case "contains" =>
        input(attributeName) contains (targetValue)
      case "like" =>
        input(attributeName) like (targetValue)

      case "rlike" =>
        input(attributeName) rlike (targetValue)
    }
  }
}

