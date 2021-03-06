package com.apple.scala

/**
 * Author: cs
 * Date: 2020/12/15 5:22 下午
 * Desc: 隐式参数
 */
object TestImplicitParameter {
  def main(args: Array[String]): Unit = {

    // 隐式参数
    implicit val s: String = "leo"

    // 普通函数
    def sayHi(name: String = "default"): Unit = {
      println(s"hello -> ${name}")
    }

    // 隐式参数
    def sayHi2(implicit name: String): Unit = {
      println(s"hello -> ${name}")
    }

    sayHi("jj")

    // 隐式参数在调用的时候，直接通过方法名调用，不需要加括号
    sayHi2
  }
}
