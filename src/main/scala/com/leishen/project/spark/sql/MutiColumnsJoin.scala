package com.leishen.project.spark.sql

import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * leishen on 2016/10/18.
  */
object MutiColumnsJoin {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("MutiColumnsJoin").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    val csvData = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", true.toString)
      .load("D:\\Hadoop\\hadoop-2.6.0\\datatest\\world.csv")

    val rightData = csvData

    val column = getJoinColumn(List(("age","age"),("height","height")),csvData,rightData)

    val result = csvData.join(rightData,column,"inner")
    result.show()
    sparkContext.stop()
  }

  private def getJoinColumn(joinColumns: List[(String, String)], left: DataFrame, right: DataFrame): Column = {

    var joinColumn = left(joinColumns(0)._1) === right(joinColumns(0)._2)

    for (i <- 1 until joinColumns.size) {
      val columnOp = left(joinColumns(i)._1) === right(joinColumns(i)._2)
      joinColumn = joinColumn && columnOp
    }
    joinColumn
  }
}
