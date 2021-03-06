package com.apple.scala

/**
 * Author: cs
 * Date: 2020/12/15 4:37 下午
 * Desc: 隐式函数
 *        - 可以动态的拓展类的功能
 *        - 当编译器第一次编译失败的时候，会在当前环境中查找能让编译通过的implicit方法，该方法用于将类型进行转换，转换之后，
 *          进行二次编译，实现对类功能的拓展
 */
object TestImplicitFunction {

  // 隐式函数: Int转换为MyRichInt
  // 使用implicit关键字声明的函数称之为隐式函数
  implicit def convertIntToMyRichInt(a:Int): MyRichInt ={
    new MyRichInt(a);
  }

  def main(args: Array[String]): Unit = {
    // 比较两个数的大小，拓展Int类的功能
    // Int类没有myMax方法，使用隐式函数拓展Int类的功能
    // 当想调用对象功能时，如果编译错误，那么编译器会尝试在当前作用域范围内查找能调用对应功能的转换规则，
    // 这个调用过程是由编译器完成的，所以称之为隐式转换。也称之为自动转换
    2.myMax(5)
    println(new MyRichInt(2).myMax(5))
  }
}


/**
 * 拓展 Int类功能
 */
class MyRichInt(var self: Int) {
  def myMax(that: Int): String = {
    if (self > that) s"${self}比${that}大" else s"${self}比${that}小"
  }
}