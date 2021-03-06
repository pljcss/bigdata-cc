package com.cc.datasetapi

import org.apache.flink.api.scala._

/**
 * Author: cs
 * Date: 2020/12/24 12:44 下午
 * Desc: map
 *       flatMap
 *       filter
 *       distinct
 *       groupBy
 *       sum
 */
object TransformationApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet1: DataSet[Int] = env.fromCollection(List(1, 2, 3, 4, 5))
    dataSet1
      .map(_ * 2)
      .filter(_ > 5)
      .print()


    val dataSet2: DataSet[String] = env.fromCollection(List("hello,world", "hello,flink"))
    dataSet2.flatMap(_.split(",")).distinct().map((_, 1)).groupBy(0).sum(1).print()
    println(dataSet2.getParallelism)


    val dataSet3: DataSet[String] = env.fromCollection(List("hello,world", "hello,flink"))



  }

}
