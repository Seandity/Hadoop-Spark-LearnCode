package com.leishen.project.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * leishen on 2016/10/20.
  */
object ConnectMySql {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("ConnectMySql")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    val url = "jdbc:mysql://localhost:3306/test"
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "199211")

    val myData = sqlContext.read.jdbc(url,"table1",prop)
    myData.show()
    sparkContext.stop()
  }

}
