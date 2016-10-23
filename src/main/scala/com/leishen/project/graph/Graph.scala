package com.leishen.project.graph

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.graphx.{Edge, Graph, GraphLoader}
import org.apache.spark.graphx.impl.EdgeRDDImpl
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * leishen on 2016/10/21.
  */
object GraphTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Graph").setMaster("local[6]")
    val sparkContext = new SparkContext(sparkConf)
    /*val tenData = sparkContext.textFile("E:\\BaiduYunDownload\\trainRepost.txt").map(line=>(line.split("\\001")(0),1)).reduceByKey(_+_)
    val top = tenData.map(line=>(line._2,line._1)).sortByKey(false,1).take(10)
    top.foreach(line=>println(line))*/

    val relationData = sparkContext.textFile("D:\\WeiBoData\\weibo_dc_parse2015_link_filter")
    val userRdd = relationData.filter(line => line.split("\\s").size == 2).flatMap(line => {
      var re = Set.empty[(Long, String)]

      val people = line.split("\\s")(0)
      val ids = line.split("\\s")(1).split("\\001")
      re += Tuple2(people.toLong, "1")
      ids.foreach(id => (
        re += Tuple2(id.toLong, "1")
        ))
      re
    }
    )
    println("point size "+userRdd.count())
    val edgRdd = relationData.flatMap(line => {
      var relations = Set.empty[Edge[String]]
      val people = line.split("\\s")(0)
      val ids = line.split("\\s")(1).split("\\001")
      for (i <- 1 until ids.length)
        relations += Edge(people.toLong, ids(i).toLong, "1")
      relations
    })
  println("edg size "+edgRdd.count())
    sparkContext.stop()


  }

  private def collectionToString(seq: Seq[String]): String = {
    val builder = new StringBuilder
    for (value <- seq)
      builder.append(value + ",")
    builder.toString()
  }

}
