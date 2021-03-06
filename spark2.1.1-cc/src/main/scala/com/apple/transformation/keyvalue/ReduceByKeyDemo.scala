package com.apple.transformation.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: cs
 * Date: 2020/12/15 7:59 下午
 * Desc: reduceByKey()算子
 */
object ReduceByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ReduceByKeyDemo")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(("a", 10), ("a", 1), ("a", 1), ("b", 1), ("b", 1), ("c", 1)))

    rdd.reduceByKey((x, y) => x + y).collect().foreach(println)

    sc.stop()
  }
}
