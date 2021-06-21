package com

/**
 * @Author: cs
 * @Date: 2021/3/19 10:39 上午
 * @Desc:
 */
object Test {
  def main(args: Array[String]): Unit = {
//    val myMap: Map[String, String] = Map("key1" -> "value")
//    println(myMap("key1"))
//    println(myMap("key2"))

    val list = List(1,2,3,4,"aaa")

    val ints = list.map {
      case x:Int => x + 1
      case _ => None
    }

    println(ints)

  }


  // 定义函数
  def foo():Unit = {
    println("foo...")
  }
}
