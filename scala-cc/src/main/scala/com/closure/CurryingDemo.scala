package com.closure

/**
 * @Author: cs
 * @Date: 2021/3/22 5:42 下午
 * @Desc:
 * 柯里化
 */
object CurryingDemo {
  // 定义方法，完成两个字符串的拼接
  // 方式1：普通写法
  def merge1(s1: String, s2: String): String = s1 + s2


  // 方式2：柯里化写法
  def merge2(s1: String, s2: String)(f1: (String, String) => String) = f1(s1, s2)

  def main(args: Array[String]): Unit = {
    val m1 = merge1("aaa", "bbb")
    println(m1)

    // 柯里化的写法，需要两个参数体
    val m2 = merge2("aaa", "bbb")(_ + _)
    println(m2)

    // 柯里化的写法更加灵活，比如要转成大写后在拼接，不需要修改方法，直接传入对应的方法即可
    val m3 = merge2("aaa", "bbb")(_.toUpperCase() + _.toUpperCase())
    println(m3)


  }

}
