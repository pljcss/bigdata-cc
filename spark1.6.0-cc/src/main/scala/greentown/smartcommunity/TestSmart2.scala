package greentown.smartcommunity

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

import java.util.Date
object TestSmart2 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("KafkaDirect").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(10))

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


//    messages.map(_._2).count().map(x=>(x,new Date().getTime)).print()


    messages.map(x=>(x._1, x._2)).map(x=>x._2).map{
      x =>

        println((new Date().getTime), x)
    }.print()


    ssc.start()
    ssc.awaitTermination()
  }
}
