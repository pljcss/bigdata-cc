package com.core.transformation.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: cs
 * Date: 2020/12/15 10:21 下午
 * Desc: combineByKey
 */
object CombineByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("CombineByKeyDemo APP")
    val sc = new SparkContext(conf)

    val list: List[(String, Int)] = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
    val scoreRDD = sc.makeRDD(list)
    //    (88+91+95)/3
    //    (95+93+98)/3

    println("--------- groupByKey ----------")
    // 每个学生的平均成绩 方案1 使用groupByKey [不是最优解，因为如果分组之后某个组数据量比较大，会造成单点压力，效率有可能会差]
    scoreRDD.groupByKey().map {
      case (key, values) =>
        (key, values.sum / values.size)
    }.collect().foreach(println)

    println("--------- reduceByKey ----------")
    // 求每个学生的平均成绩 方案2 使用 reduceByKey
    // 先转换为 (a, (88,1)) 的结构
    val mapRDD = scoreRDD.map {
      case (name, score) => (name, (score, 1))
    }
    mapRDD.reduceByKey {
      (t1, t2) => (t1._1 + t2._1, t1._2 + t2._2)
    }.map {
      case (name, scoreAll) => (name, scoreAll._1 / scoreAll._2)
    }.collect().foreach(println)

    println("--------- combineByKey ----------")
    // 求每个学生的平均成绩 方案2 使用 combineByKey
    // createCombiner: V => C,  对RDD中当前key取出第一个value做一个初始化
    // mergeValue: (C, V) => C,  分区内计算规则，主要在分区内进行，即当前分区的value值合并到初始化得到的C上面
    // mergeCombiners: (C, C) => C,  分区间计算规则
    val combineRDD: RDD[(String, (Int, Int))] = scoreRDD.combineByKey(
      (a: Int) => (a, 1),
      (tup1: (Int, Int), v) => (tup1._1 + v, tup1._2 + 1),
      (tup1: (Int, Int), tup2: (Int, Int)) => (tup1._1 + tup2._1, tup1._2 + tup2._2)
    )

    combineRDD.collect().foreach(println)

  }
}
