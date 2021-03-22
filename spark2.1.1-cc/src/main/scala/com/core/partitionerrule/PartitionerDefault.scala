package com.core.partitionerrule

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 分区个数
 *     从集合创建
 *     从外部文件系统创建
 *
 * @author cs
 * @date 2020/12/10 11:06 上午
 */
object PartitionerDefault {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("PartitionerAPP")
    val sc = new SparkContext(conf)

    // 通过集合创建RDD
    val collectionRDD = sc.makeRDD(List(1,2,3,4))
    // 查看分区效果
    println(collectionRDD.partitions.length)
    // 分区个数多少，则会保存多少个文件
//    collectionRDD.saveAsTextFile("spark2.1.1-cc/output_collection")

    // 通过外部文件创建RDD
    val fileRDD = sc.textFile("spark2.1.1-cc/input")
    // 查看分区效果
    println(fileRDD.partitions.length)
    // 分区个数多少，则会保存多少个文件
    fileRDD.saveAsTextFile("spark2.1.1-cc/output_file")


    // 关闭资源
    sc.stop()


  }

}
