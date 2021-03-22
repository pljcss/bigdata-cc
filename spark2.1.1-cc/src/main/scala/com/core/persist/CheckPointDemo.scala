package com.core.persist

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: cs
 * Date: 2020/12/16 7:05 下午
 * Desc: checkpoint
 */
object CheckPointDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("CheckPointDemo APP")
    val sc = new SparkContext(conf)

    // 设置检查点目录
    sc.setCheckpointDir("check_point")
    // 开发环境应该把检查点目录设置在HDFS上
//    System.setProperty("HADOOP_USER_NAME", "root")
//    sc.setCheckpointDir("hdfs://hadoop03:9000/check_point")

    val rdd = sc.makeRDD(List("hello world", "hello spark"), 3)

    // 扁平映射
    val flatMapRDD = rdd.flatMap(_.split(" "))

    // 结构转换
    val mapRDD = flatMapRDD.map {
      word => {
        (word, System.currentTimeMillis())
      }
    }

    // 在开发环境中，一般检查点和缓存配合使用
    mapRDD.cache()
    // 设置检查点
    mapRDD.checkpoint()

    // 打印血缘关系
    println(mapRDD.toDebugString)
    // 触发行动操作
    mapRDD.collect().foreach(println)

    println("-" * 20)

    // 打印血缘关系
    println(mapRDD.toDebugString)
    // 触发行动操作
    mapRDD.collect().foreach(println)

    sc.stop()
  }

}
