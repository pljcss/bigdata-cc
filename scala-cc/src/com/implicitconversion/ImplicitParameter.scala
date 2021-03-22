package com.implicitconversion

/**
 * @Author: cs
 * @Date: 2021/3/22 9:20 下午
 * @Desc:
 *      隐式参数
 *      - 同一个作用域中，相同类型的隐式值只能有一个
 *      - 编译器按照隐式参数的类型去寻找对应类型的隐式值，与隐式值的名称无关
 *      - 隐式参数优先于默认参数
 */
object ImplicitParameter {

  def main(args: Array[String]): Unit = {
    implicit var s: String = "阿姆斯特朗"

    def sayHi(implicit name: String): Unit = {
      println("hello-> " + name)
    }

    // 隐式参数在调用的时候，直接通过方法名称调用，不需要加括号
    sayHi


  }
}
