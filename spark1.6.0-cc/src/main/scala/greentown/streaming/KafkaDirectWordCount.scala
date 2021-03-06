package greentown.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 通过Kafka Direct方式的实时wordcount程序
  * Created by saicao on 2017/8/27.
  */
object KafkaDirectWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaDirectWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    // 首先创建一份kafka参数Map
    var kafkaParams:Map[String, String] = Map()
//    kafkaParams += ("metadata.broker.list" -> "183.134.74.26:9092,183.134.74.36:9092,183.134.76.42:9092")

    kafkaParams += ("metadata.broker.list" -> "10.0.0.120:9092,10.0.0.153:9092,10.0.0.184:9092")
    // 然后,要创建一个set,里面放入,你要读取的topic
    // 这个,就是我们所说的,它自己给你做的很好,可以并行读取多个topic
    var topics:Set[String] = Set()
    topics += "test-topic"
    // 创建输入DStream创建输入DStream
    val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)



    // Get the lines, split them into words, count the words and print
//    val lines = messages.map(_._2)
//    val words = lines.flatMap(_.split(" "))
//    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
//    wordCounts.print()


    messages.map(_._2).print()


    ssc.start()
    ssc.awaitTermination()
  }
}
