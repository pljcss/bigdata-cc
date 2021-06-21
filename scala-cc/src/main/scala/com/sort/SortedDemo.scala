package com.sort

import scala.math.Ordering

/**
 * @Author: cs
 * @Date: 2021/4/9 3:45 下午
 * @Desc:
 */
object SortedDemo {
  def main(args: Array[String]): Unit = {

    // 1、单列排序
    val list = List(1, 5, 10, 2, 4, 3)
    val listStr = List("1", "5", "10", "2", "4", "3")
    // sorted
    println(list.sorted)
    println(list.sorted.reverse)
    println(listStr.sorted) // 直接按字符串排序
    println(listStr.sorted.reverse)

    // sortBy
    println("-" * 20)
    println(list.sortBy(x => x))
    println(listStr.sortBy(x => x))
    println(listStr.sortBy(x => x.toInt))

    // sortWith
    println(list.sortWith(_.compareTo(_) < 0))

    println("-" * 20)


    // 2、双列集合排序
    val tupleList = List(("b", "8"), ("a", "10"), ("a", "2"), ("a", "3"), ("a", "1"), ("c", "2"), ("d", "1"), ("f", "8"))

    // 使用sortBy
    val tuples1 = tupleList.sortBy(x => (x._1, x._2.toInt))(Ordering.Tuple2(Ordering.String, Ordering.Int))
    println(tuples1)

    // 使用sortWith
    val tuples = tupleList.sortWith {
      case (x, y) => {
        if (x._1 == y._1) {
          x._2.toInt < y._2.toInt
        } else {
          x._1 < y._1
        }
      }
    }

    println(tuples)


  }

}
