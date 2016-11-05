package com.leishen.project.spark.mlib


import org.apache.spark.{SparkConf, SparkContext}

/**
  * leishen on 2016/10/9.
  */
object Logic {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("LogicRegression").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)

    val inputRdd = sparkContext.textFile("/home/sltest/data.txt")

  /*  val examples = inputRdd.map(line =>{
      val parts = line.split(",")

      LabeledPoint(parts(6).toDouble,Vectors.dense(parts.map(value =>value.toDouble)))
    })*/

   /* val model = LinearRegressionWithSGD.train(examples,100,0.5,0.7,Vectors.dense(Array(0.0)))*/



  }

}
