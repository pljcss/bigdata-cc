package greentown.lc.test

import java.util.Date
import scala.util.parsing.json.JSON

object ScalaTest7 {

  def main(args: Array[String]): Unit = {
    val now = new Date()
    val now_time = now.getTime
    println(now_time)
    println(Long.MaxValue-now_time)

    val str_info = "{\"apartmentId\":0,\"buildingId\":0,\"contactPhoneNum\":\"18270809203\",\"floorId\":0,\"gmtModify\":\"2018-03-09 14:02:23\",\"id\":2,\"identityCard\":\"360124199505181214\",\"isPushKey\":0,\"isRegister\":0,\"keyType\":0,\"memberId\":1594792361429303296,\"name\":\"熊豪\",\"portrait\":\"https://zos.alipayobjects.com/rmsportal/jkjgkEfvpUPVyRjUImniVslZfWPnJuuZ.png\",\"projectId\":1,\"sex\":0,\"status\":0,\"type\":1,\"updateUser\":1}"

    val b = JSON.parseFull(str_info)
    b match {
      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
      case Some(map: Map[String, Any]) => println(if (map.contains("sss")) "you" else "null")
      case None => println("Parsing failed")
      case other => println("Unknown data structure: " + other)
    }
  }

}
