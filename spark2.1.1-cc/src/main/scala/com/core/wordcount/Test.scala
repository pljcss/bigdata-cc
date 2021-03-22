package com.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: cs
 * @Date: 2021/3/22 11:08 上午
 * @Desc:
 */
object Test {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("hello", "world", "spark", "hello", "hello", "world"))
    rdd.map((_,1)).groupBy(_._1).collect().foreach(println)






    val rdd2 = sc.makeRDD(List(
      ("UserA","LocA","2018-01-09 12:00",60),
      ("UserA","LocA","2018-01-09 13:00",60),
      ("UserA","LocB","2018-01-09 11:00",20),
      ("UserA","LocA","2018-01-09 19:00",60)))



    rdd2.groupBy(x=>(x._1,x._2)).collect().foreach(println)

    sc.stop()



  }

}
