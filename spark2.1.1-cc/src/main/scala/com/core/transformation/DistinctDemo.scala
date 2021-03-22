package com.core.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author cs
 * @date 2020/12/12 11:11 上午
 */
object DistinctDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("DistinctDemo")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4,5,1,2,3), numSlices = 5)

    rdd.mapPartitionsWithIndex{
      (index,datas)=>{
        println(index + "========>>>>" + datas.mkString(","))
        datas
      }
    }.collect()

    // 去重
    val newRDD = rdd.distinct()
    newRDD.foreach(println)

    newRDD.mapPartitionsWithIndex{
      (index,datas)=>{
        println(index + "========>>>>" + datas.mkString(","))
        datas
      }
    }.collect()

    sc.stop()
  }
}
