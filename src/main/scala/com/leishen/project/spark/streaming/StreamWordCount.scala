package com.leishen.project.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by shenlei on 2016/10/30.
  */
object StreamWordCount {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("StreamWordCCount").setMaster("local[3]")
    val streamContext = new StreamingContext(sparkConf,Seconds(1))

    val dStream = streamContext.textFileStream("D:\\temp")

    val wordCount = dStream.flatMap(line=>line.split(" ")).map(word=>(word,1)).reduceByKey(_+_)

    wordCount.print()

    streamContext.start()

    streamContext.awaitTermination()
  }
}
