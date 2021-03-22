package com.core.transformation.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: cs
 * Date: 2020/12/15 8:33 下午
 * Desc: 转换算子 -aggregateByKey
 *        - 按照key对分区内以及分区间的数据进行处理
 *        - aggregateByKey(初始值)(分区内计算规则,分区间计算规则) : 柯里化的方式
 */
object AggregateByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("AggregateByKeyDemo APP")
    val sc = new SparkContext(conf)

    // 创建RDD
    val rdd: RDD[(String, Int)] =
      sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

    // 使用aggregateByKey实现 wordcount
    val resRDD = rdd.aggregateByKey(0)(
      (x, y) => x + y,
      (x, y) => x + y
    )
    resRDD.collect().foreach(println)

    // 分区数据
    rdd.mapPartitionsWithIndex((index, datas) => {
      println(s"--index: ${index}" + datas.mkString(","))
      datas
    }).collect().foreach(println)

    // 分区最大值求和
    println("-" * 20 + "分区后")
    rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    ).collect().foreach(println)

    // 分组最大值求和
    println("-" * 20 + "分组最大值")
    rdd.groupByKey().map {
      case (x, y) => (x, y.max)
    }.collect().foreach(println)

    sc.stop()

  }

}
