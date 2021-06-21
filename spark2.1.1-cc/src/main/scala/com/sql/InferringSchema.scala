package com.sql

import org.apache.spark.sql.SparkSession

/**
 * @Author: cs
 * @Date: 2021/4/10 11:40 上午
 * @Desc:
 *       使用反射方式
 */
object InferringSchema {
  // 样例类应该定义在外面，定义在里面会报错
  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    // 创建SparkSession，SparkSQL入口
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("").getOrCreate()
    // For implicit conversions from RDDs to DataFrames
    import spark.implicits._


    val dataFrame = spark.sparkContext.textFile("spark2.1.1-cc/input/people.txt").map {
      line => {
        val fields = line.split(",")
        Person(fields(0), fields(1).trim.toInt)
      }
    }.toDF()

    dataFrame.show()

    dataFrame.createOrReplaceTempView("people")
    val res = spark.sql("select name,age from people where age >20")
    res.show()

  }
}
