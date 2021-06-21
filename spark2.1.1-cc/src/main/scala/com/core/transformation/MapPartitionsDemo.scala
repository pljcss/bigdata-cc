package com.core.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author cs
 * @date 2020/12/12 11:11 上午
 */
object MapPartitionsDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("MapPartitionsDemo")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4), numSlices = 2)

    // 乘2操作
    val newRDD: RDD[Int] = rdd.mapPartitions(datas=>{
      println("--------")
      datas.map(elem=>elem*2)
    })

    newRDD.foreach(println)

    sc.stop()
  }
}
