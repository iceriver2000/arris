package com.arris.test.arris

import java.io.FileWriter

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.io.Source

/**
  * Created by bruce on 16/10/9.
  */
object OfflineTestDateFormat {

  def main(args: Array[String]): Unit = {
    val testFile="/Users/bruce/Downloads/ccf_data/ccf_offline_stage1_test.csv"
    val conf = new SparkConf().setAppName("offline-test").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(testFile).map(f=>{
      val arr = f.split(",")
      val uid = arr(0).trim
      val mid = arr(1).trim
      val cid = arr(2).trim
      val disc_rat = arr(3).trim
      val dist = arr(4).trim
      val day = arr(5).trim
      (uid,mid,cid,disc_rat,dist,day)
    })

    rdd.cache()
    val sg = "\t"
    //将用户属性聚合,增加用户总优惠券数量,按商户汇总的优惠券数量
    val rdd1 = rdd.map(f=>(f._1,1)).reduceByKey((x,y)=>x+y)
    val rdd2 = rdd.map(f=>(f._1+sg+f._2,1)).reduceByKey((x,y)=>x+y)
    val m1 = mutable.HashMap[String,Int]()
    rdd1.collect().foreach(f=>{
      m1 +=(f._1->f._2)
    })
    val m2 = mutable.HashMap[String,Int]()
    rdd2.collect().foreach(f=>{
      m2 +=(f._1->f._2)
    })

    val fw = new FileWriter("/Users/bruce/Downloads/ccf_data/f1_offline_stage1_test.csv")
    rdd.map(f=>{
      val uid = f._1
      val mid = f._2
      val cid = f._3
      val disc_rat = f._4
      val dist = f._5
      val day = f._6
      val total_c_num = m1.get(uid).get
      val m_c_num =  m2.get(uid+sg+mid).get
      val distInfo = OfflineFileConvert.getDistInfo(disc_rat)
      val dist_amount  = distInfo._1
      val dist_rat = distInfo._2
      (uid,mid,cid,total_c_num,m_c_num,dist_amount,dist_rat,disc_rat,dist,day)
    }).collect().foreach(f=>{
      fw.write(f._1+sg+f._2+sg+f._3+sg+f._4+sg+f._5+sg+f._6+sg+f._7+sg+f._8+sg+f._9+sg+f._10+"\n")
    })
   fw.flush()
    fw.close()
  }
}

