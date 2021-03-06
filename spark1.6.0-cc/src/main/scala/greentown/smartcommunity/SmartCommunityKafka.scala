package greentown.smartcommunity

import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.hadoop.mapred.JobConf

import scala.util.parsing.json.JSON

object SmartCommunityKafka {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaDirect").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val confHbase = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    confHbase.set("hbase.zookeeper.quorum","10.0.0.120,10.0.0.184,10.0.0.91")
    //设置zookeeper连接端口，默认2181
    confHbase.set("hbase.zookeeper.property.clientPort", "2181")

    val tablename = "smartcommunity"

    //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
    val jobConf = new JobConf(confHbase)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

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


//    val rddd: DStream[Any] = messages.map(_._2).map{ arr => {
//      val now = new Date()
//      val now_time = now.getTime
//      val rowkey = Long.MaxValue-now_time
//      val put = new Put(Bytes.toBytes(rowkey))
//
//      val b = JSON.parseFull(arr)
//      b match {
//        // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
////        case Some(map: Map[String, Any]) => println(map.get("name").get)
//        case Some(map: Map[String, Any]) => {
//          put.add(Bytes.toBytes("cf"),Bytes.toBytes("entranceName"),Bytes.toBytes(arr(0)))
//          put.add(Bytes.toBytes("cf"),Bytes.toBytes("openMethod"),Bytes.toBytes(arr(1)))
//          put.add(Bytes.toBytes("cf"),Bytes.toBytes("portrait"),Bytes.toBytes(arr(2)))
//          put.add(Bytes.toBytes("cf"),Bytes.toBytes("recognizeTime"),Bytes.toBytes(arr(3)))
//          put.add(Bytes.toBytes("cf"),Bytes.toBytes("residentMobile"),Bytes.toBytes(arr(4)))
//          put.add(Bytes.toBytes("cf"),Bytes.toBytes("residentName"),Bytes.toBytes(arr(5)))
//          put.add(Bytes.toBytes("cf"),Bytes.toBytes("residentType"),Bytes.toBytes(arr(6)))
//          //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
//
//        }
//        case None => println("Parsing failed")
//        case other => println("Unknown data structure: " + other)
//      }
////      (new ImmutableBytesWritable, put)
//    }}

  val ss = messages.map(_._2).print()


  println(ss.toString)

//    rddd.saveAsHadoopDataset(jobConf)





    ssc.start()
    ssc.awaitTermination()
  }

}
