package com.sql

import org.apache.spark.sql.SparkSession

/**
 * @Author: cs
 * @Date: 2021/4/10 12:51 下午
 * @Desc:
 */
object HiveTables {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("")
                                      .master("local[*]")
                                      .enableHiveSupport() // 启用hive支持
                                      .getOrCreate()



    spark.sql("show tables").show()

    spark.sql("select * from business").show()

  }
}
