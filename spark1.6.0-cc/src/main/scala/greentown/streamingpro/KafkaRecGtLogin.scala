package greentown.streamingpro

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.parsing.json.JSON

/**
  * Created by saicao on 2017/8/28.
  */
object KafkaRecGtLogin {
  // mysql
  var conn: Connection = null
  var ps: PreparedStatement = null
  val sql = "insert into craftsman_login(userKey,userID,logType,recordTime,pageKey,version,status,IPAddr,IPProvince,IPCity,macAddr,operator) values (?,?,?,?,?,?,?,?,?,?,?,?)"
  conn = DriverManager.getConnection("jdbc:mysql://183.134.74.36:3306/sparkstreamtest","root", "gt123")

  def main(args: Array[String]): Unit = {
    var map1:Map[String, String] = Map()
    val conf = new SparkConf().setAppName("KafkaRecGtLogin")//.setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    // 使用KafkaUtils.createStream()方法,创建针对Kafka的输入流数据
    var topicThreadMap:Map[String, Int] = Map()
    topicThreadMap += ("gt4s_login_test"->1)
    val lines: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      ssc,
      "183.134.74.26:2181,183.134.74.36:2181,183.134.76.42:2181",
      "DefalutConsumerGroup",
      topicThreadMap)

    // val str2 = lines.map(x=>x._2).toString

     lines.map(x=>x._2).map(a=>
       a.toString.substring(a.indexOf("message")+9, a.indexOf("tags")-2)
     ).map{
       b=>
         var bb = JSON.parseFull(b)
         bb match {
           // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
           case Some(map: Map[String, String]) => map1 = map
           case None => println("Parsing failed")
           case other => println("Unknown data structure: " + other)
         }
         println(map1.get("IPAddr") + ":" +  map1.get("userKey") + map1.get("userID") + map1.get("logType") + map1.get("recordTime"))
         ps = conn.prepareStatement(sql)
         //userKey,userID,logType,recordTime,pageKey,version,status,IPAddr,IPProvince,IPCity,macAddr,operator
         ps.setString(1, map1.get("userKey").get)
         ps.setString(2, map1.get("userID").get)
         ps.setString(3, map1.get("logType").get)
         ps.setString(4, map1.get("recordTime").get)
         ps.setString(5, map1.get("pageKey").get)
         ps.setString(6, map1.get("version").get)
         ps.setString(7, map1.get("status").get)
         ps.setString(8, map1.get("IPAddr").get)
         ps.setString(9, map1.get("IPProvince").get)
         ps.setString(10, map1.get("IPCity").get)
         ps.setString(11, map1.get("macAddr").get)
         ps.setString(12, map1.get("operator").get)
         ps.executeUpdate()
         //map1.get("IPAddr").get
     }.print()


    //lines.map(x=>x._2).print()
    ssc.start()
    ssc.awaitTermination()

  }
}
