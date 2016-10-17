package com.leishen.project.sql

import org.apache.spark.sql.{Row, SQLContext}
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

    val data =sqlContext.read.  format("com.databricks.spark.csv")
      .option("header","true")
      .option("inferSchema",true.toString)
      .load("D:\\Hadoop\\hadoop-2.6.0\\datatest\\world.csv")

    //data.show()
    val resultData = data
    val columns = resultData.columns

    println(columns)
    import sqlContext.implicits._
    resultData.filter($"age"<= 22).show()
    resultData.where($"age" <= 22 && $"height" === 22).show()
    resultData.agg(Map("age"->"max","age"->"min","age"->"variance","age"->"sum","age"->"count")).show()

    println(resultData.schema("age").dataType)
    val data12 = data.unionAll(data)
    data12.show(100)

    sparkContext.stop()

  }

}
