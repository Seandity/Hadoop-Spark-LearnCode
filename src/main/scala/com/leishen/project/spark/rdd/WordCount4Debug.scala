package com.leishen.project.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by leishen on 2016/12/5 0005.
  * 该类主要查看其源码的执行过程
  */
object WordCount4Debug {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("4debug")

    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.textFile("L:\\word.txt")

    val resultRdd = rdd.flatMap(_.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
   println(resultRdd.count())

    sparkContext.stop()
  }

}
