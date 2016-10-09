package com.arris.test.arris

import java.io.FileWriter
import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by bruce on 16/10/8.
  */
object OfflineFileConvert {
  val df=new SimpleDateFormat("yyyyMMdd")
  def main(args: Array[String]): Unit = {
      println(">>>>>>")
      val conf = new SparkConf().setAppName("offline-test").setMaster("local")
      val sc = new SparkContext(conf)
      val file = "/Users/bruce/Downloads/ccf_data/ccf_offline_stage1_train.csv"
      val rdd = sc.textFile(file).map(f => {
        val arr = f.split(",")
        if (arr.length == 7) {
          val uid = arr(0).trim
          val mid = arr(1).trim
          val cid = arr(2).trim
          val disc_rat = arr(3).trim
          val dist = arr(4).trim
          val date_rec = arr(5).trim
          val date_cons = arr(6).trim
          if (!cid.equals("null") && !date_cons.equals("null")) {
             val days = getIntervalDays(date_rec,date_cons)
             if(days>15){
               (uid, mid, cid, disc_rat, dist, date_rec, date_cons, 1,0)
             }else{
               (uid, mid, cid, disc_rat, dist, date_rec, date_cons, 1,1)
             }

          } else if (!cid.equals("null") && date_cons.equals("null")) {
            (uid, mid, cid, disc_rat, dist, date_rec, date_cons, 0,0)
          } else {
            (null, null, null, null, null, null, null, null,null)
          }
        } else {
          (null, null, null, null, null, null, null, null,null)
        }
      }).filter(f => (f._1 != null))
    rdd.cache()
    val fw = new FileWriter("/Users/bruce/Downloads/ccf_data/f1_offline_train.csv")
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

    rdd.map(f=>{
      val uid = f._1
      val mid = f._2
      val rel_dist_rat = f._4
      val total_c_num = m1.get(uid).get
      val m_c_num =  m2.get(uid+sg+mid).get
      val distInfo = getDistInfo(rel_dist_rat)
      val dist_amount  = distInfo._1
      val dist_rat = distInfo._2
      (f._1 , f._2 , f._3 , total_c_num,m_c_num,dist_amount,dist_rat ,f._4, f._5 , f._6 , f._7 ,f._8,f._9)

    }).collect().foreach(
      f => {
        fw.write(f._1 + sg + f._2 + sg + f._3 + sg + f._4 + sg + f._5 + sg + f._6 + sg + f._7 +sg+f._8+sg+f._9+ sg + f._10 +sg+f._11+sg+f._12+sg+f._13+ "\n")
      })
    fw.flush()
    fw.close()

  }

  //将优惠幅度离散,并增加消费金额限制,0 无限制;1 [0~20);2 [20~50); 3 [50~100);4 [100~max)
def getDistInfo(rel_dist_rat:String): (Int,String) ={
  var dist_rat = "0"
  var dist_amount  = 0
  if(rel_dist_rat.indexOf(":")>0){
    val arr = rel_dist_rat.split(":")
    val am1 = arr(0).trim.toInt
    val am2 = arr(1).trim.toInt
    if(am1<20){
      dist_amount=1
    }else if(am1>=20 && am1<50){
      dist_amount=2
    }else if(am1>=50 && am1<100){
      dist_amount=3
    }else{
      dist_amount=4
    }

    dist_rat =  ((am1-am2)/am1.toDouble).formatted("%.2f")

  }else{
    dist_rat = rel_dist_rat
  }
  (dist_amount,dist_rat)
}

  def getIntervalDays(start_time:String,end_Time:String)={
    val begin=df.parse(start_time)
    val end= df.parse(end_Time)
    val between=(end.getTime()-begin.getTime())/1000
    val days=between/3600/24
    days.toInt

  }
}
