package com.core.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: cs
 * @Date: 2021/3/22 1:28 下午
 * @Desc:
 * 排序
 */
object SortByDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SortByDemo")
    val sc = new SparkContext(conf)

    val numberRdd: RDD[Int] = sc.makeRDD(List(2, 1, 3, 4, 6, 5))

    // 默认生序
    numberRdd.sortBy(x => x).collect().foreach(println)

    // 降序排序
    numberRdd.sortBy(x => x, ascending = false).collect().foreach(println)

    println("-" * 20)


    // 按字典排序
    val strRdd: RDD[String] = sc.makeRDD(List("1", "22", "12", "2", "3"))
    strRdd.sortBy(x => x, ascending = false).collect().foreach(println)
    strRdd.sortBy(x => x.toInt, ascending = false).collect().foreach(println)

    println("-" * 20)

    // 先按照tuple的第一个值排序，相等再按照第2个值排
    val rdd3: RDD[(Int, Int)] = sc.makeRDD(List((2, 1), (1, 2), (1, 1), (2, 2)))

    rdd3.sortBy(t=>t).collect().foreach(println)




    sc.stop()

  }

}
