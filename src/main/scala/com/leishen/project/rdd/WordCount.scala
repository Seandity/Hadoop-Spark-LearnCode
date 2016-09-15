package com.leishen.project.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * leishen on 2016/9/15.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sparkContext = new SparkContext(sparkConf)

    val wordRdd = sparkContext.makeRDD(List("hello,world", "hello,spark", "hello,java","please,say,scala"))
    val countInfo = wordRdd.flatMap(line => line.trim.split(",")).map(word => (word, 1)).reduceByKey(_ + _).collect()
    countInfo.foreach(wordCount => {
      println("word :" + wordCount._1 + " count :" + wordCount._2)
    })

    sparkContext.stop()
  }

}
