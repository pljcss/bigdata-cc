package com.apple.createrdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author cs
 * @date 2020/12/10 10:45 上午
 */
object RDDFromCollection {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("CreateRDDAPP")
    val sc = new SparkContext(conf)

    val array = Array("a","b","c","d")

    // 从集合创建方式1
    val rdd1 = sc.parallelize(array)
    rdd1.foreach(println)

    // 从集合创建方式2
    val rdd2 = sc.makeRDD(array)
    rdd2.foreach(println)

    sc.stop()
  }
}
