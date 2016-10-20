package com.leishen.project.sql

import org.apache.spark.sql.{Column, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * leishen on 2016/9/21.
  */
object FilterAndWhere {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Hadoop\\hadoop-2.6.0\\hadoop-2.6.0");
    val conf = new SparkConf().setAppName("FilterAndWhere").setMaster("local")
    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)

    val data = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", true.toString)
      .load("D:\\Hadoop\\hadoop-2.6.0\\datatest\\world.csv")


    import sqlContext.implicits._
    val result = data.filter("age >11 and age <=22")
    println(data.schema("age"))
    result.show()



    sparkContext.stop()

  }

}
