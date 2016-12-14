package com.leishen.project.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by leishen on 2016/12/14 0014.
  */
object TestEqual {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("AddID").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)

    val sQLContext = new SQLContext(sparkContext)


    val data = sQLContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", true.toString)
      .load("S:\\word.csv")

    data.filter(data("c") === "d").show()
  }

}
