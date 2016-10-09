package com.arris.ml.dt

import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.util.MLUtils

/**
  * Created by bruce on 16/10/9.
  */
object GBDTRegression {

  def main(args: Array[String]): Unit = {
    val starttime = System.currentTimeMillis()
    val conf = new SparkConf().setAppName("arris train model").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //val data = MLUtils.loadLibSVMFile(sc, "/Users/bruce/source/spark/data/mllib/sample_libsvm_data.txt")
    val data = MLUtils.loadLibSVMFile(sc, "/Users/bruce/Downloads/ccf_data/lab_offline_train.csv")
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a GradientBoostedTrees model.
    // The defaultParams for Regression use SquaredError by default.
    val boostingStrategy = BoostingStrategy.defaultParams("Regression")
    boostingStrategy.numIterations = 10 // Note: Use more iterations in practice.
    boostingStrategy.treeStrategy.maxDepth = 6
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

    val model = GradientBoostedTrees.train(trainingData, boostingStrategy)

    // Evaluate model on test instances and compute test error
    val labelsAndPredictions = testData.map { point =>
      val prediction = model.predict(point.features)
      println(point.features+">>>>>>"+prediction)
      (point.label, prediction)
    }
    val testMSE = labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2)}.mean()
    println("Test Mean Squared Error = " + testMSE)
    //println("Learned regression GBT model:\n" + model.toDebugString)

    // Save and load model
    //model.save(sc, "/Users/bruce/Downloads/ccf_data/model")
    //val sameModel = GradientBoostedTreesModel.load(sc,"target/tmp/myGradientBoostingRegressionModel")
    val endtime = System.currentTimeMillis()
    println("end use "+(endtime-starttime) +"  ms")
  }
}
