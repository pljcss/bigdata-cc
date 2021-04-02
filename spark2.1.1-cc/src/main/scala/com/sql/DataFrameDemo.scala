package com.sql

import org.apache.spark.sql.SparkSession

/**
 * @Author: cs
 * @Date: 2021/3/23 10:12 上午
 * @Desc:
 */
object DataFrameDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL basic example")
//      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    val df = spark.read.json("spark2.1.1-cc/input/people.json")

    df.show()


  }

}
