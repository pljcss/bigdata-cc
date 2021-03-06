package greentown.lc.parsejson

import scala.util.parsing.json.JSON


/**
  * Created by saicao on 2017/8/28.
  */
object ParseJson {
  def main(args: Array[String]): Unit = {
    val str2 = "{\"path\":\"craftsman\",\"@timestamp\":\"2017-08-28T01:05:49.936Z\",\"level\":\"INFO\",\"logger\":\"com.movitech.mbox.log.craftsman.LoginLog\",\"host\":\"ecs-3135-0019.novalocal\",\"@version\":\"1\",\"source\":\"craftsman\",\"thread\":\"http-nio-8089-exec-81\",\"type\":\"login\",\"message\":{\"userKey\":\"abc1eb2a-5850-11e7-a463-fa163edf93bk\",\"userID\":\"15381110755\",\"logType\":\"login\",\"recordTime\":\"2017-08-28 09:05:49\",\"pageKey\":\"craft_homePage\",\"version\":\"1.4\",\"status\":\"1\",\"IPAddr\":\"192.168.0.115\",\"IPProvince\":\"\",\"IPCity\":\"\",\"macAddr\":\"02:00:00:00:00:00\",\"operator\":\"\"},\"tags\":[\"login\"]}"
    val str3 = str2.substring(str2.indexOf("message")+9,str2.indexOf("tags")-2)
    var map1:Map[String, String] = Map()
    val b = JSON.parseFull(str3)
    b match {
      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
      case Some(map: Map[String, String]) => map1 = map
      case None => println("Parsing failed")
      case other => println("Unknown data structure: " + other)
    }
    val ss: String = map1.get("message").toString
    println(map1.get("userKey"))

  }
}
