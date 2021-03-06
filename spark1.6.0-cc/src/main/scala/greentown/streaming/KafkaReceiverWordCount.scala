package greentown.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 基于Kafka的实时wordcount程序
  * Created by saicao on 2017/8/27.
  */
object KafkaReceiverWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaReceiverWordCount").setMaster("local")
    val ssc = new StreamingContext(conf, Seconds(5))

    // 使用KafkaUtils.createStream()方法,创建针对Kafka的输入流数据
    var topicThreadMap:Map[String, Int] = Map()
    topicThreadMap += ("wordcount"->1)
    val lines: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
                  ssc,
                 "183.134.74.26:2181,183.134.74.36:2181,183.134.76.42:2181",
                "DefalutConsumerGroup",
                topicThreadMap)
    val words = lines.map(x=>x._2).flatMap(_.split(" "))
    //val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word=>(word,1))
    val wordCounts = pairs.reduceByKey(_ + _)
    println("hehehhehe")
    wordCounts.print()
    //lines.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
