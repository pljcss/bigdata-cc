package com.core

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}


/**
 * Author: cs
 * Date: 2020/12/16 3:28 下午
 * Desc: 查看血缘关系 和依赖关系
 *        - toDebugString 查看血缘关系
 *        - dependencies 查看依赖关系
 */
object JobAnalyseDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("JobAnalyseDemo APP")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List("hello world", "hello spark"), 3)
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    // Job 一个Action算子就会触发一个Job
    // Job1 打印到控制台
    rdd.collect().foreach(println)

    // Job2 输出到磁盘
    rdd.saveAsTextFile("output_job")

    Thread.sleep(60 * 1000 * 20)

    sc.stop()

  }

}
