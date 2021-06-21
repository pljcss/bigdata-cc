package com.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
 * @Author: cs
 * @Date: 2021/4/6 7:01 下午
 * @Desc:
 *
 */
object SocketStream1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(2))

    val socketDS = ssc.socketTextStream("localhost",9999)

    socketDS.print()

    socketDS.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()

    ssc.awaitTermination()
  }

}
