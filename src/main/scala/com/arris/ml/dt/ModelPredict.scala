package com.arris.ml.dt

import java.io.FileWriter

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel

/**
  * Created by bruce on 16/10/9.
  */
object ModelPredict {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("arris test model").setMaster("local")
    val sc = new SparkContext(conf)
    //

    //val features = Vectors.dense(" ".trim().split(' ').map(java.lang.Double.parseDouble))
    val rdd = sc.textFile("/Users/bruce/Downloads/ccf_data/f1_offline_stage1_test.csv").map(f=>{
      val arr = f.split("\t")
      //(uid,mid,cid,total_c_num,m_c_num,dist_amount,dist_rat,disc_rat,dist,day)
      val uid = arr(0).trim
      val cid = arr(2).trim
      val total_c_num = arr(3).trim
      val m_c_num = arr(4).trim
      val dist_amount = arr(5).trim
      val dist_rat = arr(6).trim
      val dist = arr(8).trim
      val day = arr(9).trim
      if(dist.equals("null")){
        val indices = Array[Int](0,1,2,3)
        val values = Array[Double](total_c_num.toDouble,m_c_num.toDouble,dist_amount.toDouble,dist_rat.toDouble)
        val feature = Vectors.sparse(5,indices,values)
        (uid,cid,day,feature)
      }else{
        val indices = Array[Int](0,1,2,3,4)
        val values  = Array[Double](total_c_num.toDouble,m_c_num.toDouble,dist_amount.toDouble,dist_rat.toDouble,dist.toDouble)
        val feature = Vectors.sparse(5,indices,values)
        (uid,cid,day,feature)
      }

    })
    rdd.cache()


    val model = GradientBoostedTreesModel.load(sc,"/Users/bruce/Downloads/ccf_data/model")
    val fw = new FileWriter("/Users/bruce/Downloads/ccf_data/res_offline_test_dat.csv")
    rdd.map(f=>{
       val pred =  model.predict(f._4)
      (f._1,f._2,f._3,pred)
    }).collect().foreach(f=>{
      fw.write(f._1+","+f._2+","+f._3+","+f._4+"\n")
    })
   fw.flush()
    fw.close()


  }
}
