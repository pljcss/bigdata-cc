package com.core.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author cs
 * @date 2020/12/12 11:11 上午
 */
object GroupByDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("GroupByDemo")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4,5,6,7,8), numSlices = 2)

    // 分组
    val newRDD: RDD[(Boolean, Iterable[Int])] = rdd.groupBy(x => x < 6)
    newRDD.foreach(println)

    val rdd2 = sc.makeRDD(List("hello","hello","spark","java","java"))
    val newRDD2 = rdd2.groupBy(elem => elem)
    newRDD2.foreach(println)

    sc.stop()
  }
}
