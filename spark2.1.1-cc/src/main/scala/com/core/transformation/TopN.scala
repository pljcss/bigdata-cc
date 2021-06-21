package com.core.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: cs
 * Date: 2020/12/16 9:59 上午
 * Desc: TopN
 * 时间戳，省份，城市，用户，广告，中间字段使用空格分割
 */
object TopN {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("TopN APP")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("input/agent.log")

    rdd
      .map(line => {
        val fields = line.split(" ")
        // 组合key (省份-广告id, 1)
        (fields(1) + "-" + fields(4), 1)
      })
      .reduceByKey(_ + _)
      // (省份, (广告id, count))
      .map {
        case (provinceAndAd, clickCount) => {
          val fields = provinceAndAd.split("-")
          (fields(0), (fields(1), clickCount))
        }
      }
      .groupByKey()
      .mapValues {
        itr => {
          itr.toList.sortWith {
            (left, right) => {
              left._2 > right._2
            }
          }
        }.take(3)
      }
      .collect().foreach(println)

    sc.stop()
  }
}
