package com.apple.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: cs
 * Date: 2020/12/16 11:47 上午
 * Desc: 
 */
object ReduceDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ReduceDemo")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5))

    // reduce
    val res = rdd.reduce(_ + _)
    println(res)

    sc.stop()
  }

}
