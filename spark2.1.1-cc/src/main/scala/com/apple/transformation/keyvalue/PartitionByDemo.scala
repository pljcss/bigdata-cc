package com.apple.transformation.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
 *
 * @author cs
 * @date 2020/12/15 4:02 下午
 */
object PartitionByDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("PartitionByDemo")
    val sc = new SparkContext(conf)

    val kvRDD: RDD[(Int, String)] = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc")), 3)

    // 分区之前的分区情况
    kvRDD.mapPartitionsWithIndex((index, datas) => {
      println(index + "-" * 20 + datas.mkString(","))
      datas
    }).collect()

    println("-" * 40 + "分区" + "-" * 40)
    val newKvRDD = kvRDD.partitionBy(new MyPartitioner(2))

    // 分区之后的分区情况
    newKvRDD.mapPartitionsWithIndex((index, datas) => {
      println(index + "-" * 20 + datas.mkString(","))
      datas
    }).collect()

    sc.stop()
  }
}

class MyPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = key match {
    case 1 | 2 => 0
    case _ => 1
  }
}