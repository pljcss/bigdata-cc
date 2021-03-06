package com.apple.transformation.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: cs
 * Date: 2020/12/16 9:42 上午
 * Desc: 
 */
object JoinDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("JoinDemo APP")
    val sc = new SparkContext(conf)

    // 创建第一个RDD
    val rdd1: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c"), (1, "a")))
    // 创建第二个pairRDD
    val rdd2: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (4, 6)))

    // join相当于内连接
    rdd1.join(rdd2).collect().foreach(println)

    sc.stop()
  }

}
