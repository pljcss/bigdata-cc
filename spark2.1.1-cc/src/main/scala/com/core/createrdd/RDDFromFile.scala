package com.core.createrdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author cs
 * @date 2020/12/10 10:51 上午
 */
object RDDFromFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("App").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 从本地文件中读取数据，创建RDD
    val localFileRDD = sc.textFile("input")
    localFileRDD.collect().foreach(println)

    // 从HDFS中读取数据，创建RDD
    val hdfsRDD = sc.textFile("hdfs://hadoop03:9000/hdfs_input")
    hdfsRDD.collect().foreach(println)

    sc.stop()
  }
}
