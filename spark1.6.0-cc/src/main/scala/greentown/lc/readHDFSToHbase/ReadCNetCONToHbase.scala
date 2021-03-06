package greentown.lc.readHDFSToHbase

import java.util.Date

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/** Hbase 基本信息表, usr_bas_attr
  * 将客户的基本信息保存到hbase(usr_bas_attr)
  * Created by saicao on 2017/4/18.
  */
object ReadCNetCONToHbase {
  // 将手机号封装在ArrayBuffer中
  val arrTel = ArrayBuffer[String]()
  private val schemaString = "tel,curl,pv,region,gender,age,unknown,consume,times_mon,long_mon,inland_times,inland_long,internation_times," +
    "internation_long,net_times,net_long,net_flow,net_day_times,net_day_flow,net_day_long,net_night_times,net_night_flow,net_night_long"

  // HBase配置信息
  var put:Put = null
  val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", "dev163,dev164,dev165")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  val table = new HTable(conf, "usr_bas_attr")

  def main(args: Array[String]): Unit = {
    parseConToHbase("20170328")
  }

  /**
    * 读取hdfs上的数据,将其处理后保存到 hbase
    * @param date 日期类型 如:20170328
    */
  def parseConToHbase(date: String)={
    /**
      * 1.将HDFS上的文件,映射成表，pvTable，然后针对pvTable操作
      */
    val conf = new SparkConf().setAppName("ReadCONToHbase").setMaster("local").set("spark.debug.maxToStringFields", "100")
    val sc = new SparkContext(conf)
    val pvRDD = sc.textFile("hdfs://dev160:9000/hdfslvcheng/hdfslvcheng/lvcheng/"+ date + "/phone_label_" + date+ ".txt")
    val sqlContext = new SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame
    val schemaArray = schemaString.split(",")
    val schema = StructType(schemaArray.map(filedName => StructField(filedName, StringType, true)))

    val rowRDD = pvRDD
      .map(_.split("\t"))
      .map(eachRow => Row(eachRow(0), eachRow(1), eachRow(2), eachRow(3), eachRow(4), eachRow(5), eachRow(6), eachRow(7), eachRow(8)
        , eachRow(9), eachRow(10), eachRow(11), eachRow(12), eachRow(13), eachRow(14), eachRow(15), eachRow(16), eachRow(17)
        , eachRow(18), eachRow(19), eachRow(20), eachRow(21), eachRow(22)))

    val pvDF = sqlContext.createDataFrame(rowRDD, schema)
    // 注册为临时表
    pvDF.registerTempTable("pvTable")

    /**
      * 具体操作
      * 全局变量的声明
      */
    // 查询有手机号(并去重),并将手机号存放到ArrayBuffer
    val telDF: DataFrame = sqlContext.sql("SELECT DISTINCT tel FROM pvTable")
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
      val sql = "SELECT tel,region,gender,age " +
        "FROM pvTable " +
        "WHERE tel='" + tel + "' LIMIT 1"
      val sqlLab1 = sqlContext.sql(sql)
        .foreach { rows =>
          // 生成rowkey, 随机数-(Long.MaxValue-timestamp)-0-客户区分标识-date
          val rowkey = Math.abs(java.lang.Long.hashCode(Long.MaxValue-timestamp)) % 3 + (Long.MaxValue-timestamp).toString + "0" + "a12" + date
          put = new Put(Bytes.toBytes(rowkey))
          // put值,手机号,
          put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("encryption_tel"), Bytes.toBytes(tel.toString))
          // put值,浏览记录,通信行为
          put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("district"), if (rows(1) != null) Bytes.toBytes(rows(1).toString) else null)
          put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("gender"), if (rows(2) != null) Bytes.toBytes(rows(2).toString) else null)
          put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("age"), if (rows(3) != null) Bytes.toBytes(rows(3).toString) else null)
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
