package com.leishen.project.spark.graph

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * leishen on 2016/10/23.
  */
object GraphPractice {

  def main(args: Array[String]): Unit = {
      val sparkCOnf = new SparkConf().setAppName("GraphPractice").setMaster("local")
    val sparkContext = new SparkContext(sparkCOnf)

    val pointRDD = sparkContext.parallelize(Seq((1L,("shenlei","")),(12L,("shenlei","")),(3L,("shenlei",""))))

    val edgRDD = sparkContext.parallelize(Seq(Edge(1,12,"1"),Edge(1,3,"2"),Edge(3,12,"3")))


    val graph = Graph(pointRDD,edgRDD)

    graph.inDegrees.collect().foreach(println(_))

    sparkContext.stop()
  }

}
