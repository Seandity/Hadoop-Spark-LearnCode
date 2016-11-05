package com.leishen.project.spark.sql

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * leishen on 2016/10/17.
  */
object RddToDataFrame {
  def main(args: Array[String]): Unit = {

    val structFields = List(StructField("age",DoubleType),StructField("height",DoubleType))
    val types = StructType(structFields)

    val sparkConf = new SparkConf().setAppName("RDDToDataFrame").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    val rdd = sparkContext.textFile("D:\\Hadoop\\hadoop-2.6.0\\datatest\\world.csv")

    val rowRdd = rdd.map(line=>Row(line.trim.split(",")(0).toDouble,line.trim.split(",")(1).toDouble))

    val df = sqlContext.createDataFrame(rowRdd,types)
    df.show()





  }
}


