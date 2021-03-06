package greentown.lc.readHDFSToHbase2

import scala.collection.mutable

/**
  * 工具类
  *
  * Created by saicao on 2017/4/15.
  */
object ConstantUtils {
  val map = mutable.HashMap[String, String]("CU001" -> "finance", "CU002" -> "sport", "CU003" -> "science",
    "CU004" -> "life", "CU005" -> "entertainment", "CU006" -> "education", "CU007" -> "political",
    "CU008" -> "shopping", "CU009" -> "reading", "CU010" -> "video", "CU011" -> "music", "CU012" -> "game",
    "CU013" -> "portal", "CU014" -> "illegal_site", "CU015" -> "region", "CU016" -> "access_way", "CU017" -> "others")
  var str: String = null

  def main(args: Array[String]): Unit = {
    println(parseLabToString("CU017"))
  }

  /**
    * 将一级标签转换为对应的英文
    *
    * @param lab 一级标签  CI001
    * @return 一级标签对应的值,如finance
    */
  def parseLabToString(lab: String): String = {

    for (i <- map) {
      if (lab.equals(i._1.toString)) {
        str = i._2.toString
      }
    }
    str
  }
}