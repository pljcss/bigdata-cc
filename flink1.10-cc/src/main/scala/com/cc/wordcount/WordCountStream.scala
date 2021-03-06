package com.cc.wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * Author: cs
 * Date: 2020/12/17 3:15 下午
 * Desc: 流处理 无界流
 */
object WordCountStream {
  def main(args: Array[String]): Unit = {
    // 创建流处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 从程序中读取hostname和port
    // Program参数 --hostname localhost --port 9999
    val params = ParameterTool.fromArgs(args)
    val hostname = params.get("hostname")
    val port = params.getInt("port")


    // 读取 Socket数据
    val inputDataStream: DataStream[String] = env.socketTextStream(hostname, port)

    val res = inputDataStream
      .flatMap(_.split(" "))
      .map((_,1))
      // 按第1个元素分组
      // 无界流没有groupBy算子
      .keyBy(0)
      // 按照第2个元素求和
      .sum(1)

    res.print().setParallelism(1)
    // 输出 前面的3和2代表分区号或线程号
    // 3> (world,1)
    // 2> (hello,1)
//    res.print().setParallelism(1)

    // 无界流需要启动
    env.execute("WordCountStream APP")

  }

}
