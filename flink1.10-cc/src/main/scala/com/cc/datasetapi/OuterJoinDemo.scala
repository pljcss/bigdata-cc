package com.cc.datasetapi

import org.apache.flink.api.scala._

/**
 * Author: cs
 * Date: 2020/12/24 2:30 下午
 * Desc: leftOuterJoin()
 *       rightOuterJoin()
 *       fullOuterJoin()
 */
object OuterJoinDemo {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet1 = env.fromCollection(List(
      ("1", "北京"),
      ("2", "上海"),
      ("3", "广州"),
      ("4", "深圳")
    ))

    val dataSet2 = env.fromCollection(List(
      ("1", "beijing"),
      ("2", "shanghai"),
      ("3", "guangzhou"),
      ("5", "hangzhou")

    ))

    dataSet1.leftOuterJoin(dataSet2).where(0).equalTo(0).apply((left, right) => {
      if (right == null) {
        (left._1, left._2, "null")
      } else {
        (left._1, left._2, right._2)
      }
    }).print()

    println("*" * 30)

    dataSet1.rightOuterJoin(dataSet2).where(0).equalTo(0).apply((left, right) => {
      if (left == null) {
        (right._1, right._2, "null")
      } else {
        (right._1, right._2, left._2)
      }
    }).print()

    println("*" * 30)

    dataSet1.fullOuterJoin(dataSet2).where(0).equalTo(0).apply((left, right) => {
      if (left == null) {
        (right._1, right._2, "null")
      } else if (right == null) {
        (left._1, left._2, "null")
      } else {
        (right._1, right._2, left._2)
      }
    }).print()


  }

}
