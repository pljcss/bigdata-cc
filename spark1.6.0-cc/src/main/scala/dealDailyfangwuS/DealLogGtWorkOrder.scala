package dealDailyfangwuS

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.util.parsing.json.JSON

/**
  * Created by saicao on 2017/9/1.
  */
object DealLogGtWorkOrder {
  var conn: Connection = null
  var ps: PreparedStatement = null
  val sql = "insert into craftsman_submit_ways(user_key,user_id,record_time,widget_key,page_key,from_page_key,version,os,phone_version,os_type,is_app,log_type,order_id,order_type,repair_main_type,repair_type,repair_subtype,weakness,describe1,is_photo,ways,material,is_photo1,need_time,start_time,price,is_new_schedule,status) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
  conn = DriverManager.getConnection("jdbc:mysql://183.134.74.36:3306/gt4s","root", "gt123")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DealLogGtWorkOrder").setMaster("local")
    val sc = new SparkContext(conf)

    //val lines = sc.textFile("/home/gt4s/greentown_4s_log/craftsman/login/" + args(0))
    val lines = sc.textFile("C:\\Users\\saicao\\Desktop\\greentown_4s_log\\craftsman\\workOrder\\*")


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
        ps.setString(13, if (map1.contains("orderId")) map1.get("orderId").get else "")
        ps.setString(14, if (map1.contains("orderType")) map1.get("orderType").get else "")
        ps.setString(15, if (map1.contains("repairMainType")) map1.get("repairMainType").get else "")
        ps.setString(16, if (map1.contains("repairType")) map1.get("repairType").get else "")
        ps.setString(17, if (map1.contains("repairSubtype")) map1.get("repairSubtype").get else "")
        ps.setString(18, if (map1.contains("weakness")) map1.get("weakness").get else "")
        ps.setString(19, if (map1.contains("describe")) map1.get("describe").get else "")
        ps.setString(20, if (map1.contains("isPhoto")) map1.get("isPhoto").get else "")
        ps.setString(21, if (map1.contains("ways")) map1.get("ways").get else "")
        ps.setString(22, if (map1.contains("material")) map1.get("material").get else "")
        ps.setString(23, if (map1.contains("isPhoto1")) map1.get("isPhoto1").get else "")
        ps.setString(24, if (map1.contains("needTime")) map1.get("needTime").get else "")
        ps.setString(25, if (map1.contains("startTime")) map1.get("startTime").get else "")
        ps.setString(26, if (map1.contains("price")) map1.get("price").get else "")
        ps.setString(27, if (map1.contains("isNewSchedule")) map1.get("isNewSchedule").get else "")
        ps.setString(28, if (map1.contains("status")) map1.get("status").get else "")
        ps.executeUpdate()
      //map1.get("IPAddr").get
    }

    rr.collect()
    // 关闭
    sc.stop()
  }
}
