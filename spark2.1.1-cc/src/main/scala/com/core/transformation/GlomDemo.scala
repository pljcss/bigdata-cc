package com.core.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author cs
 * @date 2020/12/12 11:11 上午
 */
object GlomDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("GlomDemo")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6), numSlices = 2)

    // glom()操作
    val newRDD: RDD[Array[Int]] = rdd.glom()

    val newRDD2: RDD[Int] = newRDD.map(_.max)

    newRDD2.foreach(println)

    sc.stop()
  }
}
