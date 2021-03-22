package com.patternmatch

/**
 * @Author: cs
 * @Date: 2021/3/20 5:46 下午
 * @Desc:
 *       匹配数组
 *       scala模式匹配可以对集合进行精确的匹配，例如匹配只有两个元素的、且第一个元素为0的数组
 */
object MatchArrayDemo {
  def main(args: Array[String]): Unit = {

    // 对一个数组集合进行遍历
    for (arr <- Array(Array(0), Array(1, 0), Array(0, 1, 0), Array(1, 1, 0), Array(1, 1, 0, 1), Array("hello", 90))) {
      val result = arr match {
        // 匹配Array(0) 这个数组
        case Array(0) => "0"
        // 匹配有两个元素的数组，然后将将元素值赋给对应的x,y
        case Array(x, y) => x + "," + y
        // 匹配以0开头和数组
        case Array(0, _*) => "以0开头的数组"

        case _ => "something else"
      }

      println(result)
    }


  }

}
