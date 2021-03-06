package com.apple.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author cs
 * @date 2020/12/12 11:11 上午
 */
object FilterDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("FilterDemo")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List("hello","spark","java"), numSlices = 2)

    // 过滤
    val newRDD = rdd.filter(_.contains("hello"))


    newRDD.foreach(println)

    sc.stop()
  }
}
