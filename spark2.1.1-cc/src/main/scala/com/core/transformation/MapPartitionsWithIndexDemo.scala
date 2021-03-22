package com.core.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author cs
 * @date 2020/12/12 11:11 上午
 */
object MapPartitionsWithIndexDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("MapPartitionsWithIndexDemo")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4,5,6,7,8),numSlices = 3)

    // 乘2操作
    val newRDD = rdd.mapPartitionsWithIndex((index,datas)=>datas.map(elem=>(index, elem)))
    newRDD.foreach(println)

    // 需求，对第二个分区乘2，其他分区不变
    println("-------------")
    val newRDD2 = rdd.mapPartitionsWithIndex((index,partition)=>{
      index match {
        case 2 => partition.map(elem=>(index,elem*2))
        case _ => partition.map(elem=>(index,elem))
      }
    })
    newRDD2.foreach(println)


    println("-------------")
    val newRDD3 = rdd.mapPartitionsWithIndex((index, datas) => {
      index match {
        case 2 => datas.map(_ * 2)
        case _ => datas
      }
    })
    newRDD3.foreach(println)


    sc.stop()
  }
}
