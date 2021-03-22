package com.core.readandsave

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: cs
 * Date: 2020/12/16 9:05 下午
 * Desc: 
 */
object ReadMySQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ReadMySQL APP")
    val sc = new SparkContext(conf)


  }
}
