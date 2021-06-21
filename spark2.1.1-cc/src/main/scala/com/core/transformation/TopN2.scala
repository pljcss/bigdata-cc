package com.core.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: cs
 * @Date: 2021/4/8 7:11 下午
 * @Desc: TopN
 *        时间戳，省份，城市，用户，广告，中间字段使用空格分割
 *        20190212 辽宁 沈阳 张三 AAA
 *        统计出每一个省，广告被点击top3
 *
 */
object TopN2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("TopN2 APP")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("spark2.1.1-cc/input/agent2.log")

    rdd.collect().foreach(println)

    rdd.map(x => {
      val splits = x.split(" ")
      (splits(1) + "-" + splits(4), 1)
    })
      .reduceByKey(_ + _)
      .map {
        case (x, y) => {
          val res = x.split("-")
          (res(0), (res(1), y))
        }
    }.groupByKey().mapValues(x => x.toList.sortWith(_._2 > _._2).take(2))
      .collect().foreach(println)


    sc.stop()
  }
}
