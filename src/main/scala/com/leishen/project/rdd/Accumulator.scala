package com.leishen.project.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * leishen on 2016/10/8.
  */
object Accumulator {
  def main(args: Array[String]): Unit = {
    //累加器
    val sparkConf = new SparkConf().setAppName("Accumulator").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)

    val acc = sparkContext.accumulator(10)

    val rdd = sparkContext.parallelize(List(1, 2, 3, 4, 5, 6, 6, 2, 34, 234, 234, 23, 4, 523, 45, 23, 5, 234))

    val result = rdd.map(value => {
      if (value > 3)
        acc += 1
      value + 1
    })

    result.saveAsTextFile("D:\\Hadoop\\hadoop-2.6.0\\datatest\\accumulator.txt")

    println("accumulator result is " + acc)
  }

}
