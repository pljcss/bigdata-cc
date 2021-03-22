package com.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author cs
 * @date 2020/12/12 5:45 下午
 */
object WordCount2 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount2")
    val sc = new SparkContext(conf)

    // wordcount 1
    val rdd = sc.makeRDD(List("hello word","hello java", "hello spark", "spark"))
    // 使用特定算子实现（ 不使用reduceByKey()算子 ）
    val groupRDD: RDD[(String, Iterable[String])] = rdd.flatMap(_.split(" ")).groupBy(elem=>elem)
    val resRDD = groupRDD.map {
      case (word, datas) => (word, datas.size)
    }
    resRDD.collect().foreach(println)


    println("-"*100)
    // wordcount 2
    val rdd2 = sc.makeRDD(List(("hello word",2),("hello java",3), ("hello spark",2), ("spark",2)))
    val newRDD2 = rdd2.flatMap {
      case (words, count) => words.split(" ").map((word => (word, count)))
    }

    val groupRDD2 = newRDD2.groupBy(elem => elem._1)
    val resRDD2 = groupRDD2.map {
      case (word, datas) => (word, datas.map(_._2).sum)
    }
    resRDD2.collect().foreach(println)


  }

}
