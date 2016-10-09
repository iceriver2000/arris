package com.arris.test

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.Date

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by bruce on 16/9/28.
  */
object SimpleTest {

  def main(args: Array[String]): Unit = {
    val indices = Array[Int](0,1,2,3)
    val values = Array[Double](0.1,1.1,2.1,3.1)
    val f = Vectors.sparse(5, indices, values)
    println(f)


  }


}
