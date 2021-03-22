package com.regex

/**
 * @Author: cs
 * @Date: 2021/3/20 4:29 下午
 * @Desc:
 *       Scala正则表达式 与 模式匹配
 */
object RegexDemo {
  def main(args: Array[String]): Unit = {
    val ss1 = """local\[([0-9]+|\*)]""".r

    val ss2 = "local[2]"
    val ss22 = "xxxx"

    ss2 match {
      // ss1(threads) 表示提取正则括号里面的内容
      case ss1(threads) => println(threads)
      case _ => println("nothing")
    }


    Option

  }

}
