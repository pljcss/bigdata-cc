package com.cc.datasetapi

import org.apache.flink.api.scala._

/**
 * Author: cs
 * Date: 2020/12/23 7:52 下午
 * Desc: 
 */
object ReadData {

  case class MyCaseClass(name: String, age: Double, sex: String)

  case class MyCaseClass2(name: String, sex: String)

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    dataFromCsv(env)

  }


  def dataFromCollection(env: ExecutionEnvironment): Unit = {
    env.fromCollection(1 to 10).print()
  }

  def dataFromTextFile(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    env.readTextFile("/Users/saicao/IdeaProjects/bigdata-cc/flink1.10-cc/src/main/resources/word.txt").print()
  }

  def dataFromCsv(env: ExecutionEnvironment): Unit = {
    // 使用 Tuple解析
    //    env.readCsvFile[(String, Int, String)](
    //      "/Users/saicao/IdeaProjects/bigdata-cc/flink1.10-cc/src/main/resources/stu.csv",
    //      ignoreFirstLine = true
    //    ).print()

    // 使用 case class解析
    env.readCsvFile[MyCaseClass2](
      "/Users/saicao/IdeaProjects/bigdata-cc/flink1.10-cc/src/main/resources/stu.csv",
      ignoreFirstLine = true,
      includedFields = Array(0, 2)
    ).print()

  }

}


