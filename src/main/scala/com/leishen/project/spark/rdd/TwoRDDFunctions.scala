package com.leishen.project.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * leishen on 2016/10/14.
  */
object TwoRDDFunctions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("TwoRDDFunctions")
    val context = new SparkContext(conf)

    val rdd1 = context.parallelize(List(1,3,2,4,1))
    val rdd2 = context.parallelize(List(2,34,234,234,22))

    val cartesianRDD = rdd1.cartesian(rdd2)
    cartesianRDD.collect().foreach(value=>println("cartesian: "+value))

    val unionRDD = rdd1.union(rdd2)
    unionRDD.collect().foreach(value =>println("union: "+value))

    val subRDD = rdd1.subtract(rdd2)
    subRDD.collect().foreach(value =>println("sub: "+value))

    val zipRDD =rdd1.zip(rdd2)
    zipRDD.collect().foreach(value =>println("zip: "+value))



    context.stop()
  }

}
