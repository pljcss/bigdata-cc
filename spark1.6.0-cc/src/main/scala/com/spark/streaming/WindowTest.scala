package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val testDstream = ssc.socketTextStream("localhost", 9988)


    testDstream.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
