package com.cc.datastreamapi.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * Author: cs
 * Date: 2020/12/27 6:46 下午
 * Desc: 从kafka读取数据
 */
object KafkaSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

//    env.addSource() addSource()的入参是函数？？？？

    val kafkaSource = env.addSource(new FlinkKafkaConsumer011[String](
      "test111-topic",
      new SimpleStringSchema(),
      properties))

    kafkaSource.print()

    env.execute("KafkaSource Job")
  }
}
