package com.leishen.project.mlib


import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * leishen on 2016/9/16.
  */
object VectorPractice {
  def main(args: Array[String]): Unit = {
    val denseDatas = Array(1.0,2,3,4,5)
    val dense = Vectors.dense(denseDatas)
    println("this is dense vector "+dense)
    val denseLabel = LabeledPoint(1,dense)
    println("this is labelpoint feature "+denseLabel.features+" this is label "+denseLabel.label)

    val xiDatas = Array(5.0,6,7,8,9)

    val xiVector = Vectors.sparse(5,Array(0,1,2,3,4),xiDatas)
    val xiLabelPoint = LabeledPoint(2,xiVector)
    println("this is xi  labelpoint feature "+xiLabelPoint.features+" this is label "+xiLabelPoint.label)


  }

}
