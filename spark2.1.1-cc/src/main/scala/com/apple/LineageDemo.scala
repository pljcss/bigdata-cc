package com.apple

import org.apache.spark.{SparkConf, SparkContext}


/**
 * Author: cs
 * Date: 2020/12/16 3:28 下午
 * Desc: 查看血缘关系 和依赖关系
 *        - toDebugString 查看血缘关系
 *        - dependencies 查看依赖关系
 */
object LineageDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("LineageDemo APP")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List("hello world", "hello spark"),3)
    println(rdd.toDebugString)
    println(rdd.dependencies)
    println("-" * 20)

    val flatMapRDD = rdd.flatMap(_.split(" "))
    println(flatMapRDD.toDebugString)
    println(flatMapRDD.dependencies)
    println("-" * 20)

    val mapRDD = flatMapRDD.map((_, 1))
    println(mapRDD.toDebugString)
    println(mapRDD.dependencies)
    println("-" * 20)

    val resRDD = mapRDD.reduceByKey(_ + _)
    println(resRDD.toDebugString)
    println(resRDD.dependencies)
    println("-" * 20)


    sc.stop()

  }

}
