package com.arris.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by bruce on 16/9/28.
  */
object SimpleTest {

  def main(args: Array[String]): Unit = {
      println(">>>>>>")
    val conf = new SparkConf().setAppName("UserAppTrailJob").setMaster("local")
    val sc = new SparkContext(conf)
    var  file = "/Users/bruce/study/hadoop_info/hadoop/yarn-env.sh"
    sc.textFile(file).collect().foreach(f=>println(f))

  }
}
