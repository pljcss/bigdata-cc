package greentown.lc.readHDFSToHbase

import java.util.Date

import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  *  join 表phone_live和phone_work数据,
  *  将其保存到hbase
  *  Created by saicao on 2017/4/17.
  */
object ReadPOIToHbase {
  var arrTel = ArrayBuffer[String]()
  var mapLab1 = new scala.collection.mutable.HashMap[String, String]
  private val schemaStringLive = "tel,longitude,latitude"
  private val schemaStringWork = "tel,longitude,latitude"

  val factory = new JsonNodeFactory(false)
  var node1: ObjectNode = null
  var nodee: ObjectNode = null
  var put:Put = null

  // hbase配置信息
  val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", "dev163,dev164,dev165")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  val table = new HTable(conf, "usr_poi_info")

  def main(args: Array[String]): Unit = {
    parsePOIToHbase("20170328")
  }

  def parsePOIToHbase(date: String)={
    /**
      * 1.将HDFS上的文件,映射成表，pvTable，然后针对pvTable操作
      */
    val conf = new SparkConf().setAppName("ReadPOIToHbase")/*.setMaster("local")*/.set("spark.debug.maxToStringFields", "100")
    val sc = new SparkContext(conf)
    val pvRDDLive = sc.textFile("hdfs://dev160:9000/hdfslvcheng/hdfslvcheng/lvcheng/"+ date + "/phone_live_" + date+ ".txt")
    val pvRDDWork = sc.textFile("hdfs://dev160:9000/hdfslvcheng/hdfslvcheng/lvcheng/"+ date + "/phone_work_" + date+ ".txt")
    val sqlContext = new SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame
    // 注册生活区表 phone_live
    val schemaArrayLive = schemaStringLive.split(",")
    val schemaLive = StructType(schemaArrayLive.map(filedName => StructField(filedName, StringType, true)))
    val rowRDDLive = pvRDDLive
      .map(_.split("\t"))
      .map(eachRow => Row(eachRow(0), eachRow(1), eachRow(2)))
    val pvDFLive = sqlContext.createDataFrame(rowRDDLive, schemaLive)
    // 注册为生活区临时表
    pvDFLive.registerTempTable("pvLiveTable")

    // 注册工作区表 phone_work
    val schemaArrayWork = schemaStringWork.split(",")
    val schemaWork = StructType(schemaArrayWork.map(filedName => StructField(filedName, StringType, true)))
    val rowRDDWork = pvRDDWork
      .map(_.split("\t"))
      .map(eachRow => Row(eachRow(0), eachRow(1), eachRow(2)))
    val pvDFWork = sqlContext.createDataFrame(rowRDDWork, schemaWork)
    // 注册为工作区临时表
    pvDFWork.registerTempTable("pvWorkTable")

    val resLive = sqlContext.sql("select * from pvLiveTable")
    val resWork = sqlContext.sql("select * from pvWorkTable")

    resLive.show()
    resWork.show()


    /**
      * 具体操作
      */
    resLive.join(resWork, resLive("tel")===resWork("tel"), "outer").show()

    // 手机号 + pvLiveTable经度 + pvLiveTable维度 + 手机号 + pvWorkTable经度 + pvWorkTable维度
    resWork.join(resLive, resLive("tel")===resWork("tel"), "outer").show()
    /**
      * 将数据插入Hbase, 如果tel不为null，则有pvLiveTable数据
      */
    resLive.join(resWork, resLive("tel")===resWork("tel"), "outer").foreach { rows =>
      println(rows(0)+ "::" + rows(1)+ "::" + rows(2)+ "::" + rows(3)+ "::" + rows(4)+ "::" + rows(5)+ "::" )

      // 如果生活区和工作区都不为
      if(rows(0) !=null && !"".equals(rows(0)) && rows(3) !=null) {
        // 生成rowkey, 随机数-(Long.MaxValue-timestamp)-0-客户区分标识-date
        val now = new Date()
        val timestamp: Long = now.getTime
        val rowkey = java.lang.Long.hashCode(timestamp) % 3 + (Long.MaxValue-timestamp).toString + "0" + "a12" + date
        put = new Put(Bytes.toBytes(rowkey))
        // put值,手机号,
        put.addColumn(Bytes.toBytes("poi"), Bytes.toBytes("encryption_tel"), Bytes.toBytes(rows(0).toString))
        // put值,工作区坐标，使用字符串(119.89992, 28.45428)
        put.addColumn(Bytes.toBytes("poi"), Bytes.toBytes("p_work"), if (rows(4) != null && rows(5) != null) Bytes.toBytes(rows(4).toString + "," + rows(5).toString) else null)
        // put值,生活区坐标
        put.addColumn(Bytes.toBytes("poi"), Bytes.toBytes("p_live"), if (rows(1) != null && rows(2) != null) Bytes.toBytes(rows(1).toString + "," + rows(2).toString) else null)
        table.put(put)
        table.flushCommits()
      } else if(rows(0) !=null && !"".equals(rows(0)) && rows(3) == null ) {
        // 生成rowkey, 随机数-(Long.MaxValue-timestamp)-0-客户区分标识-date
        val now = new Date()
        val timestamp: Long = now.getTime
        val rowkey = java.lang.Long.hashCode(timestamp) % 3 + (Long.MaxValue-timestamp).toString + "0" + "a12" + date
        put = new Put(Bytes.toBytes(rowkey))
        // put值,手机号,
        put.addColumn(Bytes.toBytes("poi"), Bytes.toBytes("encryption_tel"), Bytes.toBytes(rows(0).toString))
        // put值,工作区坐标，使用字符串(119.89992, 28.45428)
        put.addColumn(Bytes.toBytes("poi"), Bytes.toBytes("p_work"), if (rows(4) != null && rows(5) != null) Bytes.toBytes(rows(4).toString + "," + rows(5).toString) else null)
        // put值,生活区坐标
        put.addColumn(Bytes.toBytes("poi"), Bytes.toBytes("p_live"), if (rows(1) != null && rows(2) != null) Bytes.toBytes(rows(1).toString + "," + rows(2).toString) else null)
        table.put(put)
        table.flushCommits()
      } else if (rows(0) == null && rows(3) !=null && !"".equals(rows(3))) {
        // 生成rowkey, 随机数-(Long.MaxValue-timestamp)-0-客户区分标识-date
        val now = new Date()
        val timestamp: Long = now.getTime
        val rowkey = Math.abs(java.lang.Long.hashCode(Long.MaxValue-timestamp)) % 3 + (Long.MaxValue-timestamp).toString + "0" + "a12" + date
        put = new Put(Bytes.toBytes(rowkey))
        // put值,手机号,
        put.addColumn(Bytes.toBytes("poi"), Bytes.toBytes("encryption_tel"), Bytes.toBytes(rows(3).toString))
        // put值,工作区坐标，使用字符串(119.89992, 28.45428)
        put.addColumn(Bytes.toBytes("poi"), Bytes.toBytes("p_work"), if (rows(4) != null && rows(5) != null) Bytes.toBytes(rows(4).toString + "," + rows(5).toString) else null)
        // put值,生活区坐标
        put.addColumn(Bytes.toBytes("poi"), Bytes.toBytes("p_live"), if (rows(1) != null && rows(2) != null) Bytes.toBytes(rows(1).toString + "," + rows(2).toString) else null)
        table.put(put)
        table.flushCommits()
      }

    }
    sc.stop()
  }
}
