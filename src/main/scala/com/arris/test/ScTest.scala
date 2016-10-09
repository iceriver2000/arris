package com.arris.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by bruce on 16/10/9.
  */
object ScTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("arris train model").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //val data = MLUtils.loadLibSVMFile(sc, "/Users/bruce/source/spark/data/mllib/sample_libsvm_data.txt")
     val path="/Users/bruce/source/spark/data/mllib/sample_libsvm_data.txt"
    val parsed = sc.textFile(path, 1)
      .map(_.trim)
      .filter(line => !(line.isEmpty || line.startsWith("#")))
      .map { line =>
        val items = line.split(' ')
        val label = items.head.toDouble
        val (indices, values) = items.tail.filter(_.nonEmpty).map { item =>
          val indexAndValue = item.split(':')
          val index = indexAndValue(0).toInt - 1 // Convert 1-based indices to 0-based.
        val value = indexAndValue(1).toDouble
          (index, value)
        }.unzip

        // check if indices are one-based and in ascending order
        var previous = -1
        var i = 0
        val indicesLength = indices.length
        while (i < indicesLength) {
          val current = indices(i)
          require(current > previous, "indices should be one-based and in ascending order" )
          previous = current
          i += 1
        }

        (label, indices.toArray, values.toArray)
      }

  }
}
