package dealDailyfangwuS

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.util.parsing.json.JSON

/**
  * Created by saicao on 2017/9/1.
  */
object DealLogGtPage {
  var conn: Connection = null
  var ps: PreparedStatement = null
  val sql = "insert into craftsman_page(user_key,user_id,record_time,widget_key,page_key,from_page_key,version,os,phone_version,os_type,is_app,log_type,enter_time,leave_time,stay_time) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
  conn = DriverManager.getConnection("jdbc:mysql://183.134.74.36:3306/gt4s","root", "gt123")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DealLogGT").setMaster("local")
    val sc = new SparkContext(conf)

    //val lines = sc.textFile("/home/gt4s/greentown_4s_log/craftsman/login/" + args(0))
    val lines = sc.textFile("C:\\Users\\saicao\\Desktop\\greentown_4s_log\\craftsman\\page\\*")


    var map1:Map[String, String] = Map()

    val rr: RDD[Int] = lines.map(a=>
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
        //println(map1.get("IPAddr") + ":" +  map1.get("userKey") + map1.get("userID") + map1.get("logType") + map1.get("recordTime"))
        ps = conn.prepareStatement(sql)
        //userKey,userID,logType,recordTime,pageKey,version,status,IPAddr,IPProvince,IPCity,macAddr,operator
        ps.setString(1, if (map1.contains("userKey")) map1.get("userKey").get else "")
        ps.setString(2, if (map1.contains("userID")) map1.get("userID").get else "")
        ps.setString(3, if (map1.contains("recordTime")) map1.get("recordTime").get else "")
        ps.setString(4, if (map1.contains("widgetKey")) map1.get("widgetKey").get else "")
        ps.setString(5, if (map1.contains("pageKey")) map1.get("pageKey").get else "")
        ps.setString(6, if (map1.contains("fromPageKey")) map1.get("fromPageKey").get else "")
        ps.setString(7, if (map1.contains("version")) map1.get("version").get else "")
        ps.setString(8, if (map1.contains("os")) map1.get("os").get else "")
        ps.setString(9, if (map1.contains("phoneVersion")) map1.get("phoneVersion").get else "")
        ps.setString(10, if (map1.contains("osType")) map1.get("osType").get else "")
        ps.setString(11, if (map1.contains("isApp")) map1.get("isApp").get else "")
        ps.setString(12, if (map1.contains("logType")) map1.get("logType").get else "")
        ps.setString(13, if (map1.contains("enterTime")) map1.get("enterTime").get else "")
        ps.setString(14, if (map1.contains("leaveTime")) map1.get("leaveTime").get else "")
        ps.setString(15, if (map1.contains("stayTime")) map1.get("stayTime").get else "")
        ps.executeUpdate()
      //map1.get("IPAddr").get
    }
    rr.collect()
    // 关闭
    sc.stop()
  }
}
