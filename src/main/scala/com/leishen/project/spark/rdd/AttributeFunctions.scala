package com.leishen.project.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * leishen on 2016/10/14.
  */
object AttributeFunctions {
  def main(args: Array[String]): Unit = {
    //这里直接使用RDD的Api方法来计算属性列聚合函数
    //只有当Rdd的元素是Double的时候，才会隐式的有下面的聚合函数方法
    //max,min,avg,variance等等

    val conf = new SparkConf().setAppName("AttributeFunctions").setMaster("local")
    val context = new SparkContext(conf)
    val rdd = context.textFile("D:\\Hadoop\\hadoop-2.6.0\\datatest\\world.csv")
    val doubleRdd = rdd.map(line => (line.split(",")(0).toDouble))

    val max = doubleRdd.max()
    println("最大值为: "+max)
    val min = doubleRdd.min()
    println("最小值为: "+min)
    val avg = doubleRdd.mean()
    println("平均值为: "+avg)
    val sum = doubleRdd.sum()
    println("和为: "+sum)
    val variance = doubleRdd.variance()
    println("方差为: "+variance)
    val std = doubleRdd.stdev()
    println("标准差为: "+std)
    //还有样本方差等别的函数，可以自己尝试


    context.stop()
  }

}
