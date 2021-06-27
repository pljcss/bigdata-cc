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


    // 高阶函数，f1的返回类型是函数
    def f1(msg1:String) = (msg2:String) => print(msg1 + msg2)

    f1("h1")("h2")

  }


  // 定义函数
  def foo():Unit = {
    println("foo...")
  }
}
