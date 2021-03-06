package com.apple.transformation.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: cs
 * Date: 2020/12/16 9:22 上午
 * Desc: 
 */
object MapValuesDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("MapValuesDemo APP")
    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (1, "d"), (2, "b"), (3, "c")))

    // 对value进行操作，使用map
    rdd.map {
      case (key, value) => (key, "|||" + value)
    }.collect().foreach(println)

    // 对value进行操作，使用
    rdd.mapValues("|||" + _).collect().foreach(println)

    sc.stop()
  }

}
