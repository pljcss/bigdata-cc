package greentown.smartcommunity

import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Try
import scala.util.parsing.json.JSON


object TestSmart {
  // HBase配置信息
  var put:Put = null
  val confH = HBaseConfiguration.create()
  confH.set("hbase.zookeeper.quorum", "10.0.0.120,10.0.0.184,10.0.0.91")
  confH.set("hbase.zookeeper.property.clientPort", "2181")
  val table = new HTable(confH, "smartcommunity")
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaDirect")//.setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))



    // 首先创建一份kafka参数Map
    var kafkaParams:Map[String, String] = Map()
    //    kafkaParams += ("metadata.broker.list" -> "183.134.74.26:9092,183.134.74.36:9092,183.134.76.42:9092")

    kafkaParams += ("metadata.broker.list" -> "10.0.0.120:9092,10.0.0.153:9092,10.0.0.184:9092")
    // 然后,要创建一个set,里面放入,你要读取的topic
    // 这个,就是我们所说的,它自己给你做的很好,可以并行读取多个topic
    var topics:Set[String] = Set()
//    topics += "test-topic"
    topics += "SmartCommunity-Topic"
    // 创建输入DStream创建输入DStream
    val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)


    messages.map(_._2).map{ arr => {
      val now = new Date()
      val now_time = now.getTime
      val rowkey = Long.MaxValue-now_time
      val put = new Put(Bytes.toBytes(rowkey))

      val b = JSON.parseFull(arr)
      b match {
        // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
//        case Some(map: Map[String, Any]) => println(map.get("name").get)
        case Some(map: Map[String, String]) => {
//          println(map.get("entranceName").get)
          // if (map.contains("sss")) "you" else "null"
          put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("entranceName"),Bytes.toBytes(if (map.contains("entranceName")) map.get("entranceName").get else "null"))
          put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("openMethod"),Bytes.toBytes(if (map.contains("openMethod")) map.get("openMethod").get else "null"))
          put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("portrait"),Bytes.toBytes(if (map.contains("portrait")) map.get("portrait").get else "null"))
          put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("projectId"),Bytes.toBytes(if (map.contains("projectId")) map.get("projectId").get else "null"))
          put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("recognizeTime"),Bytes.toBytes(if (map.contains("recognizeTime")) map.get("recognizeTime").get else "null"))
          put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("residentId"),Bytes.toBytes(if (map.contains("residentId")) map.get("residentId").get  else "null"))
          put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("residentMobile"),Bytes.toBytes(if (map.contains("residentMobile")) map.get("residentMobile").get else "null"))
          put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("residentName"),Bytes.toBytes(if (map.contains("residentName")) map.get("residentName").get else "null"))
          put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("buildingName"),Bytes.toBytes(if (map.contains("buildingName")) map.get("buildingName").get else "null"))
          put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("residentType"),Bytes.toBytes(if (map.contains("residentType")) map.get("residentType").get  else "null"))
          //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
          table.put(put)
          table.flushCommits()
        }
        case None => println("Parsing failed")
        case other => println("Unknown data structure: " + other)
      }
    }}.print()


    ssc.start()
    ssc.awaitTermination()


  }
}
