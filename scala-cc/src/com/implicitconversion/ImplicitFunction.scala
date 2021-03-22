package com.implicitconversion

/**
 * @Author: cs
 * @Date: 2021/3/22 9:08 下午
 * @Desc:
 *        隐式函数
 *        - 可以动态扩展类的功能
 *        - 当编译器第一次编译失败的时候，会在当前环境中查找能让编译通过的方法，该方法用于将类型进行转换，
 *          转换之后进行二次编译，实现对类功能的扩展。需要在函数面前加 implicit关键字
 */
object ImplicitDemo1 {

  // 将Int类型数据，转换为MyRichInt
  // 使用implicit关键字声明的函数称之为隐式函数
  implicit def convert(a: Int): MyRichInt = {
    new MyRichInt(a)
  }

  /**
   * 当想调用对象功能时，如果编译错误，那么编译器会尝试在当前作用域范围内查找能调用对应功能的转换规则，
   * 这个调用过程是由编译器完成的，所以称之为隐式转换。也称之为自动转换
   */
  def main(args: Array[String]): Unit = {
    println(2.myMax(5))
  }
}

// 通过隐式转换，动态扩展Int函数的功能
class MyRichInt(var self: Int) {

  def myMax(i: Int): Unit = {
    if (i > self) i else self
  }

  def myMin(i: Int): Unit = {
    if (i > self) self else i
  }

}