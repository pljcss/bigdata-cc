package com.cc.datasetapi

import org.apache.flink.api.scala._

/**
 * Author: cs
 * Date: 2020/12/24 1:31 下午
 * Desc: join()
 */
object JoinDemo {
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
      ("3", "guangzhou")
    ))

    dataSet1.join(dataSet2).where(0).equalTo(0).print()

    dataSet1.join(dataSet2).where(0).equalTo(0).apply((left, right)=>{
      (left._1, left._2, right._2)
    }).print()

  }
}
