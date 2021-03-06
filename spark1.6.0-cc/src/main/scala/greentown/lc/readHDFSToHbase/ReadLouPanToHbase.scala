package greentown.lc.readHDFSToHbase

import java.util.Date

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * 读取HDFS上phone_loupan的数据,
  * 保存到Hbase  testloupan
  * 注意: 存储比成功,Hbase中汉子存储成【\xE9\x93\xB6\xE6\x9D\x8F\xE6\xB1\x87】
  * Hbase中不能存储汉字
  * Created by saicao on 2017/4/18.
  */
object ReadLouPanToHbase {
  // 将手机号封装在ArrayBuffer中
  val arrTel = ArrayBuffer[String]()
  private val schemaString = "tel,loupan"

  // HBase配置信息
  var put:Put = null
  val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", "dev163,dev164,dev165")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  val table = new HTable(conf, "testloupan")

  def main(args: Array[String]): Unit = {
    parseLouPanToHbase("20170328")
  }

  /**
    * 将HDFS上的phone_loupan的数据保存到hbase
    * @param date 传入日期
    */
  def parseLouPanToHbase(date: String)={
    /**
      *  1.将HDFS上的phone_loupan,映射成表louPanTable，
      *  然后针对louPanTable操作
      */
    val conf = new SparkConf().setAppName("ReadLouPanToHbase").setMaster("local").set("spark.debug.maxToStringFields", "100")
    val sc = new SparkContext(conf)
    val pvRDD = sc.textFile("hdfs://dev160:9000/hdfslvcheng/hdfslvcheng/lvcheng/"+ date + "/phone_loupan_" + date+ ".txt")
    val sqlContext = new SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame
    val schemaArray = schemaString.split(",")
    val schema = StructType(schemaArray.map(filedName => StructField(filedName, StringType, true)))

    val rowRDD = pvRDD
      .map(_.split("\t"))
      .map(eachRow => Row(eachRow(0), eachRow(1)))

    val pvDF = sqlContext.createDataFrame(rowRDD, schema)
    // 注册为临时表
    pvDF.registerTempTable("louPanTable")

    /**
      * 具体操作
      * 全局变量的声明
      */
    // 查询有手机号(并去重),并将手机号存放到ArrayBuffer
    val telDF: DataFrame = sqlContext.sql("SELECT DISTINCT tel FROM louPanTable")
    // 将手机号存入arrTel
    telDF.foreach { tels => {
      println(tels(0))
      arrTel += tels(0).toString
      //println(arr(0))
    }
    }
    // 遍历手机号,根据手机号查询每个用户的 通信信息
    for (tel <- arrTel) {
      // 生成时间
      val now = new Date()
      val timestamp: Long = now.getTime
      // 查询每个用户的 通信信息 取一条信息
      val sql = "SELECT tel,loupan " +
                "FROM louPanTable " +
                "WHERE tel='" + tel + "'"
      val sqlLab1 = sqlContext.sql(sql)
        .foreach { rows =>
          // 生成rowkey, 随机数-(Long.MaxValue-timestamp)-0-客户区分标识-date
          val rowkey = Math.abs(java.lang.Long.hashCode(Long.MaxValue-timestamp)) % 3 + (Long.MaxValue-timestamp).toString + "0" + "a12" + date
          put = new Put(Bytes.toBytes(rowkey))
          // put值,手机号,
          put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("encryption_tel"), Bytes.toBytes(tel.toString))
          // put值,浏览记录,通信行为
          put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("district"), if (rows(1) != null) Bytes.toBytes(rows(1).toString) else null)
          table.put(put)
          table.flushCommits()
        }
    }
    // 清空ArrayBuffer
    arrTel.clear()
    // 关闭资源
    sc.stop()
  }
}
