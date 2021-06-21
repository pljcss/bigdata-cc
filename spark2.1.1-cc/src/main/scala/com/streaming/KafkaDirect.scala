package com.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: cs
 * @Date: 2021/4/16 10:52 下午
 * @Desc: 消费kafka数据 - direct方式
 */
object KafkaDirect {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaDirect")
    val ssc = new StreamingContext(conf, Seconds(5))

    val stream = KafkaUtils.createDirectStream(ssc, Map("" -> ""), Set("hello-topic"))


  }

}
