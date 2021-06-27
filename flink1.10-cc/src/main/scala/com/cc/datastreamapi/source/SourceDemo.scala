package com.cc.datastreamapi.source

import org.apache.flink.streaming.api.scala._

/**
 * Author: cs
 * Date: 2020/12/27 6:36 下午
 * Desc: 从集合、文件读取
 *      流式从文件、集合中读取数据，与批的方式的区别
 */
// 定义样例类，传感器id，时间戳，温度
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

//    env.setParallelism(1)

    // 1、从集合读取
    val collectionSource = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    ))

//    collectionSource.print()

    // print是一种特殊的sink
    collectionSource.print("ssss111")

//    // 2、从文件读取
//    val fileSource = env.readTextFile("/Users/saicao/IdeaProjects/bigdata-cc/flink1.10-cc/src/main/resources/sensor.txt")
//    fileSource.print()
//
//    // 3、从文件读取
//    val socketSource = env.socketTextStream("localhost", 9999)
//    socketSource.print()

    // 执行
    env.execute("stream test")
  }

}
