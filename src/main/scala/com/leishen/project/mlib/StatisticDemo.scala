package com.leishen.project.mlib

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkConf, SparkContext}

/**
  * leishen on 2016/9/17.
  */
object StatisticDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Statistic").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val data = List("1,2,3,4","1,3,4,5","5,6,7,8","1,1,1,1")

    val rdd = sparkContext.parallelize(data)

    val changeRdd = rdd.map(line=>line.split(",")).map(dataSeq=>{
     dataSeq.map(value=>value.toDouble)
    }).map(doubleValue=>Vectors.dense(doubleValue))

    val statisticResult = Statistics.colStats(changeRdd)

    println("this is max "+statisticResult.max)
    println("this is min "+statisticResult.min)
    println("this is avg "+statisticResult.mean)
    println("this is count "+statisticResult.count)
    println("this is variance "+statisticResult.variance)
    println("this is L1 "+statisticResult.normL1)
    println("this is L2 " + statisticResult.normL2)
  }

}
