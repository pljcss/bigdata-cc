package com.core.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: cs
 * @Date: 2021/3/22 1:01 下午
 * @Desc:
 *       重新分区：coalesce、repartition
 *       扩大分区（注意，默认情况下使用coalesce扩大分区是不起作用的，因为默认没shuffle）
 *       - coalesce ： 一般用于缩减分区，默认不进行shuffle
 *       - repartition ：
 *              一般用于扩大分区
 *              底层调用的就是 coalesce(numPartitions, shuffle = true)，默认执行shuffle，
 *
 */
object CoalesceDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("CoalesceDemo")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6), 3)

    // 查看每个分区的数据
    rdd.mapPartitionsWithIndex((index, datas) => {
      println(index + " =====>>> " + datas.mkString(","))
      datas
    }).collect()

    // 缩减分区
    val newRDD = rdd.coalesce(2)

    // 查看coalesce后的分区的数据
    newRDD.mapPartitionsWithIndex((index, datas) => {
      println(index + " =====>>> " + datas.mkString(","))
      datas
    }).collect()


    // 扩大分区（注意，默认情况下使用coalesce扩大分区是不起作用的，因为默认没shuffle）
    // 如果要扩大分区，使用repartition()
    rdd.coalesce(4)

    rdd.repartition(4)

    sc.stop()

  }

}
