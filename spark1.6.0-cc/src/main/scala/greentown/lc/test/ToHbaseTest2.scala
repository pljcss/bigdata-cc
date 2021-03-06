package greentown.lc.test

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by saicao on 2017/7/19.
  */
object ToHbaseTest2 {
  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val tablename = "smartcommunity"

    sc.hadoopConfiguration.set("hbase.zookeeper.quorum","gt120,gt184,gt91")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    val job = new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val indataRDD = sc.makeRDD(Array("11,jack,15","21,Lily,16","31,mike,16"))
    //val indataRDD = sc.textFile("hdfs://nameservice1/test/rddtest.txt")
    //val indataRDD = sc.textFile("hdfs://nameservice1/lcdmp/new_l_view_st_c/20170610/000000_0")
//    val indataRDD = sc.textFile("file:///C:\\Users\\saicao\\Desktop\\c_test.txt")
//
//    val rr: RDD[Array[String]] = indataRDD.map(_.split('\001'))
//    val rr1: RDD[String] = indataRDD.flatMap(_.split('\001'))
//    println(rr.foreach(x => println(x)))


    val rdd = indataRDD.map(_.split(',')).map{ arr=>{
      val put = new Put(Bytes.toBytes(arr(0)))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("age"),Bytes.toBytes(arr(2)))
      (new ImmutableBytesWritable, put)
    }}

    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())


  }



}
