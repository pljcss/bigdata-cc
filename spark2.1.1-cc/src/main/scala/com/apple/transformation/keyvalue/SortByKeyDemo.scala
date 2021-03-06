package com.apple.transformation.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: cs
 * Date: 2020/12/16 8:56 上午
 * Desc: 
 */
object SortByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SortByKeyDemo APP")
    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((3, "aa"), (6, "cc"), (2, "bb"), (1, "dd")))

    // key默认规则进行排序
    rdd.sortByKey().collect().foreach(println)

    // 自定义key排序规则
    val stdList = List(
      (new Student("aa", 19), 1),
      (new Student("aa", 18), 1),
      (new Student("aa", 20), 1),
      (new Student("bb", 19), 1),
      (new Student("cc", 19), 1),
      (new Student("bb", 16), 1)
    )

    val rdd2 = sc.makeRDD(stdList)
    rdd2.sortByKey().collect().foreach(println)


  }
}

// 指定排序规则
class Student(var name: String, var age: Int) extends Ordered[Student] with Serializable {
  override def compare(that: Student): Int = {
    // 先按name进行排序, 再按age进行排序
    if (!this.name.equals(that.name)) {
      this.name.compareTo(that.name)
    } else {
      this.age.compareTo(that.age)
    }
  }

  override def toString: String = s"Student(${name}, ${age})"
}
