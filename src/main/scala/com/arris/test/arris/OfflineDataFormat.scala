package com.arris.test.arris

import java.io.FileWriter

import scala.io.Source

/**
  * Created by bruce on 16/10/9.
  */
object OfflineDataFormat {
  def main(args: Array[String]): Unit = {
     val sg = " "
    val fw = new FileWriter("/Users/bruce/Downloads/ccf_data/lab_offline_train.csv")
    Source.fromFile("/Users/bruce/Downloads/ccf_data/f1_offline_train.csv").getLines().foreach(f=>{
      val arr = f.split("\t")
      val t_c_num = arr(3).trim
      val m_c_num = arr(4).trim
      val dist_amount = arr(5).trim
      val dist_rat = arr(6)
      val  distance = arr(8).trim
      val res = arr(12)
      var out = ""
      out +=res+sg+"1:"+t_c_num+sg+"2:"+m_c_num+sg+"3:"+dist_amount+sg+"4:"+dist_rat
      if(!distance.equals("null")){
        out+=sg+"5:"+dist_amount
      }
      fw.write(out+"\n")

     })
    fw.flush()
    fw.close()
  }
}
