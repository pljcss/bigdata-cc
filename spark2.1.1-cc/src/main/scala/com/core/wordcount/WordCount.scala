package com.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author cs
 * @date 2020/12/9 9:32 下午
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCountApp")
    val sc: SparkContext = new SparkContext(conf)
//
    val textRDD: RDD[String] = sc.textFile("spark2.1.1-cc/input/1.txt")

    println(textRDD.getNumPartitions)

    textRDD.map(_.split(" ")).foreach(x=>x.foreach(println))


//    val flatMapRDD: RDD[String] = textRDD.flatMap(_.split(" "))
//
//    val mapRDD: RDD[(String, Int)] = flatMapRDD.map((_,1))
//
//    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
//
//    reduceRDD.collect().foreach(println)

    // args(0) : input/   args(1) : output/
//    sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile(args(1))

    sc.stop()

  }
}
