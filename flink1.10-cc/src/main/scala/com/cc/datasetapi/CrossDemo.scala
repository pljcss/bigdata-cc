package com.cc.datasetapi

import org.apache.flink.api.scala._

/**
 * Author: cs
 * Date: 2020/12/24 2:52 下午
 * Desc: cross() : 笛卡尔积
 */
object CrossDemo {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val dataSet1 = env.fromCollection(List("a", "b", "c"))
    val dataSet2 = env.fromCollection(List("1", "2", "3"))

    // 笛卡尔积
    dataSet1.cross(dataSet2).print()
  }
}
