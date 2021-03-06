package com.cc.datasetapi

import org.apache.flink.api.scala._

/**
 * Author: cs
 * Date: 2020/12/24 2:55 下午
 * Desc: mapPartition()
 */
object MapPartitionDemo {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val dataSet: DataSet[String] = env.fromCollection(List("hello world", "hello java")).setParallelism(2)

    dataSet.mapPartition(x=>{
//      x.map((_,1))
      x.flatMap(_.split(" "))
    }).print()

    println(dataSet.getParallelism)
  }

}
