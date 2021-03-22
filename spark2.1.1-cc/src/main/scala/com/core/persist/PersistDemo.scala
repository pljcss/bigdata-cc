package com.core.persist

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: cs
 * Date: 2020/12/16 7:05 下午
 * Desc: cache persist
 */
object PersistDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("PersistDemo APP")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List("hello world", "hello spark"), 3)

    // 扁平映射
    val flatMapRDD = rdd.flatMap(_.split(" "))

    // 结构转换
    val mapRDD = flatMapRDD.map {
      word => {
        println("********")
        (word, 1)
      }
    }

    // 对RDD的数据进行缓存，底层调用的是 persist函数 默认缓存再内存中
//    mapRDD.cache()
//    mapRDD.persist()
    // persist可以接收参数，指定缓存位置
    // 注意：虽然叫持久化，但是当应用程序执行结束之后，缓存的目录也会被删除；只有当前节点能看到缓存数据，其他节点看不到别的节点缓存的数据
    // 实际DISK_ONLY使用并不多
    mapRDD.persist(StorageLevel.DISK_ONLY)

    // 打印血缘关系
    println(mapRDD.toDebugString)
    // 触发行动操作
    mapRDD.collect()

    println("-" * 20)

    // 打印血缘关系
    println(mapRDD.toDebugString)
    // 触发行动操作（没有缓存rdd，会再次执行mapRDD，即会打印 println("********")）
    // 缓存后map不会再打印 println("********")
    mapRDD.collect()

    sc.stop()
  }

}
