package com.core.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author cs
 * @date 2020/12/12 11:11 上午
 */
object MapDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("MapDemo")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4), numSlices = 2)

    // 乘2操作
    val newRDD = rdd.map(_*2)

    newRDD.foreach(println)

    sc.stop()
  }
}
