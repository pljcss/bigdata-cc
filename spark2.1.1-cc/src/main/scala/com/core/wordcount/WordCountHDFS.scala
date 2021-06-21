package com.core.wordcount

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: cs
 * @Date: 2021/4/16 10:10 上午
 * @Desc:
 */
object WordCountHDFS {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCountHDFS")

    val sc = new SparkContext(conf)

    val rdd = sc.textFile(args(0))

    rdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile(args(1))

    sc.stop()
  }

}
