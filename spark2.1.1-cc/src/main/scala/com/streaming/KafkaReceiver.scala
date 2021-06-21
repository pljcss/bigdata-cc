package com.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: cs
 * @Date: 2021/4/16 4:38 下午
 * @Desc: 消费kafka数据 - receiver方式
 */
object KafkaReceiver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaStreaming")
    val ssc = new StreamingContext(conf, Seconds(5))

    val kafkaStream = KafkaUtils.createStream(ssc,
                            "192.168.5.3:2181",
                            "group-ssc-1",
                            Map("hello-topic" -> 1))

    kafkaStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
