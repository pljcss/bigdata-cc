package com.apple.transformation.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: cs
 * Date: 2020/12/15 8:00 下午
 * Desc: 
 */
object GroupByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("GroupByKeyDemo APP")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(("a", 1), ("b", 5), ("a", 5), ("b", 2)))

    rdd.groupBy(_._1).collect().foreach(println)
    println("-" * 20)
    rdd.groupByKey().collect().foreach(println)

    val value: RDD[(String, Iterable[Int])] = rdd.groupByKey()

    // 求和
    value.map{
      case (index, datas) => (index, datas.sum)
    }.collect().foreach(println)

    sc.stop()

  }
}
