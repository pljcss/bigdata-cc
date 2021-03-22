package com.closure

/**
 * @Author: cs
 * @Date: 2021/3/19 7:49 下午
 * @Desc:
 */
object ClosureDemo {
  def main(args: Array[String]): Unit = {

    /**
     * (Int) => (Int) : 表示函数的返回值
     */
    def f1(): (Int) => (Int) = {
      var a: Int = 10

      def f2(b:Int)={
        a + b
      }
      f2 _

    }



  }
}
