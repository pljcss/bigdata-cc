package com.core.accumulator

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: cs
 * @Date: 2021/4/9 8:15 下午
 * @Desc:
 * 累加器
 */
object AccumulatorDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("AccumulatorDemo")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)))

    // 使用shuffle进行求和
    //    rdd.reduceByKey(_+_).collect().foreach(println)

    // 不使用reduceByKey
    var sum = 0
    rdd.foreach(x => {
      sum += x._2
      println("executor:::" + sum)
    })
    // 计算不出来
    println(sum)

    // 使用累加器
    val sumAccumulator = sc.longAccumulator("sumAccumulator")

    rdd.foreach(x => {
      sumAccumulator.add(x._2)
    })

    print("sumAccumulator " + sumAccumulator.value)

    sc.stop()

  }
}
