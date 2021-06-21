package com.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: cs
 * @Date: 2021/4/16 8:38 上午
 * @Desc:
 */
object SocketStream2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCountNc2")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    val socket = ssc.socketTextStream("localhost", 9999)

    socket.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()


    ssc.start()
    ssc.awaitTermination()


  }

}
