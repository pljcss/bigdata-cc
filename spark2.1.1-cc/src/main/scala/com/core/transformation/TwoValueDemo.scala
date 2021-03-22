package com.core.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: cs
 * @Date: 2021/3/22 1:57 下午
 * @Desc:
 *
 * 并集 ：union
 * 交集 ：intersection
 * 差集 ：subtract
 * 拉链 ：zip
 */
object TwoValueDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("TwoValueDemo")
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2 = sc.makeRDD(List(4, 5, 6, 7))

    // 并集
    val unionRdd = rdd1.union(rdd2)
    unionRdd.collect().foreach(println)

    println("-" * 20)

    // 交集
    val intersectionRdd = rdd1.intersection(rdd2)
    intersectionRdd.collect().foreach(println)

    println("-" * 20)

    // 差集
    val subtractRdd = rdd1.subtract(rdd2)
    subtractRdd.collect().foreach(println)

    println("-" * 20)

    // 拉链
    val zipRdd = rdd1.zip(rdd2)
    zipRdd.collect().foreach(println)

    sc.stop()
  }
}
