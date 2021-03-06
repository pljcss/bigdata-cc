package com.cc.wordcount

import org.apache.flink.api.scala._

/**
 * Author: cs
 * Date: 2020/12/17 1:49 下午
 * Desc: 批处理，有界流
 */
object WordCountBatch {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 读取数据
    val inputDataSet: DataSet[String] = env.readTextFile(
      "/Users/saicao/IdeaProjects/bigdata-cc/flink1.10-cc/src/main/resources/word.txt")

    val res = inputDataSet
      .flatMap(_.split(" "))
      .map((_, 1))
      // 以二元组的第1个元素作为key分组
      .groupBy(0)
      // 聚合二元组中的第二个元素的值
      .sum(1)

    // 打印
    res.print()

  }
}
