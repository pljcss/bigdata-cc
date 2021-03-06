package com.apple.transformation.keyvalue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * Author: cs
 * Date: 2020/12/15 9:40 下午
 * Desc: foldByKey 可以理解为是aggregateByKey的简化版，即分区内的计算规则和分区间的计算规则相同
 */
object FoldByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("FoldByKeyDemo APP")
    val sc = new SparkContext(conf)

    // 创建RDD
    val rdd: RDD[(String, Int)] =
      sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

    // 如果分区内和分区间的计算规则一样，且不需要执行初始值，则优先使用 reduceByKey
    println("--------- reduceByKey -------------")
    rdd.reduceByKey(_ + _).collect().foreach(println)

    // 如果分区内和分区间的计算规则一样，并且需要指定初始值，则优先使用 foldByKey
    println("--------- foldByKey -------------")
    rdd.foldByKey(0)(_ + _).collect().foreach(println)

    // 如果分区内和分区间的计算规则不一样，并且需要指定初始值，则优先使用 aggregateByKey
    println("--------- aggregateByKey -------------")
    rdd.aggregateByKey(0)(_ + _, _ + _).collect().foreach(println)

    sc.stop()

  }
}
