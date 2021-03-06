package greentown.lc.test

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by saicao on 2017/7/19.
  */
object ToHbaseTest4 {

  val tableName="lc_c_test"

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("HBaseWriteTest")
    val sc = new SparkContext(sparkConf)
    val readFile =sc.textFile("hdfs://nameservice1/lcdmp/new_l_view_st_c/20170610/000000_0").map(x=>x.split("\001"))


    readFile.foreach{
      y=>{
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum","gt79,gt91,gt44")
        conf.set("hbase.defaults.for.version.skip", "true")
        conf.set("hbase.zookeeper.property.clientPort", "2181")

        val myTable=new HTable(conf,tableName)
        myTable.setAutoFlush(false, false)//关键点1
        myTable.setWriteBufferSize(3*1024*1024)//关键点2
        val p=new Put(Bytes.toBytes(y(0)))
        p.add(Bytes.toBytes("cf"),Bytes.toBytes("l"),Bytes.toBytes(y(1)))
        myTable.put(p)
        myTable.flushCommits()//关键点3
      }
    }

    sc.stop()

  }

}
