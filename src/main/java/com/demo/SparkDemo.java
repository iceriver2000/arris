package com.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

/**
 * Created by bruce on 16/9/28.
 */
public class SparkDemo {
     public static void main(String[] args){
         System.out.println(">>>>>>>>");
          //int spark  context
          SparkConf conf = new SparkConf().setAppName("spark job name").setMaster("local");
          JavaSparkContext context = new JavaSparkContext(conf);

          //read file and out put
          String file="/Users/bruce/study/hadoop_info/hadoop/yarn-env.sh";
          JavaRDD<String> rdd = context.textFile(file);
          List<String> data  = rdd.collect();
          for (int i = 0; i < data.size(); i++) {
               System.out.println(data.get(i));
          }
     }
}
