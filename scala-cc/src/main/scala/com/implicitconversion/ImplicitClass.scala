package com.implicitconversion

/**
 * @Author: cs
 * @Date: 2021/3/22 9:31 下午
 * @Desc: 隐式类
 */
object ImplicitClass {
  def main(args: Array[String]): Unit = {

    println(2.myMax(5))
  }


  /**
   * 隐式类
   */
  implicit class MyRichInt(var self: Int) {

    def myMax(i: Int): Unit = {
      if (i > self) i else self
    }

    def myMin(i: Int): Unit = {
      if (i > self) self else i
    }

  }

}
