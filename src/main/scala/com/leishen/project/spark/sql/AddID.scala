package com.leishen.project.spark.sql

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * leishen on 2016/10/25.
  */
object AddID {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("AddID").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)

    val sQLContext = new SQLContext(sparkContext)


    val data = sQLContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", true.toString)
      .load("D:\\Hadoop\\hadoop-2.6.0\\datatest\\world.csv")
    val size = data.collect().size

    val id = sQLContext.range(0+5,size+5).rdd

   /* id.*/
    val scheme = data.schema
    val addId = StructField("id",LongType) +: scheme

    val sche = StructType(addId)


    val haha = data.rdd

    val join = id.zip(haha)

    val idRdd = join.map(value=>Row.fromSeq(value._1.toSeq ++ value._2.toSeq))

    val result = sQLContext.createDataFrame(idRdd,sche)
    result.show()
    sparkContext.stop()
  }
}
