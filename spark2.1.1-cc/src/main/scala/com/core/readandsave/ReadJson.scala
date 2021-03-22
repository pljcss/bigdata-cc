package com.core.readandsave

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
 * Author: cs
 * Date: 2020/12/16 8:36 下午
 * Desc: 
 */
object ReadJson {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ReadJsonDemo APP")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("input/test.json")
    val resRDD = rdd.map(JSON.parseFull)

    resRDD.foreach(println)

    sc.stop()
  }

}
