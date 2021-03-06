package com.apple.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author cs
 * @date 2020/12/12 11:11 上午
 */
object FlatMapDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("FlatMapDemo")
    val sc = new SparkContext(conf)

    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1,2),List(3,4),List(5,6),List(7,8)), numSlices = 2)

    // 扁平化
    val newRDD = rdd.flatMap(datas=>datas)

    newRDD.foreach(println)


    sc.stop()
  }
}
