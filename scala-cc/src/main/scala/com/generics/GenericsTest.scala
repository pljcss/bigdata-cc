package com.generics

/**
 * @Author: cs
 * @Date: 2021/4/9 8:44 下午
 * @Desc:
 *       泛型
 */
object GenericsTest {
  def main(args: Array[String]): Unit = {
    val s1: MyList[Child] = new MyList[Child]
    // 泛型协变，可以放子类型
    val s2: MyList1[Child] = new MyList1[SubChild]
    // 逆变，可以放父类型
    val s3: MyList2[Child] = new MyList2[Parent]

    // 下界
    test(classOf[Child])
    test(classOf[Parent])
//    test(classOf[SubChild]) // 运行时报错
//    test2(classOf[Parent]) // 运行时报错
  }

  // 泛型通配符，下界（需要传入Child的父类型）
  def test[A >: Child](a:Class[A]): Unit = {
  }

  // 泛型通配符，上界（需要传入Child的子类型）
  def test2[A <: Child](a:Class[A]): Unit = {
  }

}

// 泛型模版
// 不可变性
class MyList[T] {}

// 协变（加号表示协变，除了放本身类型外，还可以放子类型）
class MyList1[+T] {}

// 逆变（减号表示逆变，除了放本身类型外，还可以放父类型）
class MyList2[-T] {}

class Parent{}
class Child extends Parent {}
class SubChild extends Child {}