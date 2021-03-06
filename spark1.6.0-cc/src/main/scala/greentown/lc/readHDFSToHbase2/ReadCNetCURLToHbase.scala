package greentown.lc.readHDFSToHbase2

import java.util.Date

import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/** Hbase 用户C网行为表 usr_c_net_bhvr
  *
  * 先查询出手机号,然后根据手机号，查询每个手机号的url,pv
  *
  * 统计出这个手机号中第一标签(去重)，将其存放[是否用map],集合中的每一个一级标签依次遍历url,如果有的话，
  * 将其存入json{"4":{"CU017002001":"3",""CU017002002:"1"}}
  *
  * 存入hbase,
  *
  * Created by saicao on 2017/4/13.
  */
object ReadCNetCURLToHbase {
  // scala中声明全局变量在main方法外
  // 将手机号封装在ArrayBuffer中
  val arrTel = ArrayBuffer[String]()
  val mapLab1 = new scala.collection.mutable.HashMap[String, String]
  private val schemaString = "tel,curl,pv,region,gender,age,unknown,consume,times_mon,long_mon,inland_times,inland_long,internation_times," +
                              "internation_long,net_times,net_long,net_flow,net_day_times,net_day_flow,net_day_long,net_night_times,net_night_flow,net_night_long"
  // JackSon
  val factory = new JsonNodeFactory(false)
  var node1: ObjectNode = null
  var nodee: ObjectNode = null

  // HBase配置信息
  var put:Put = null
  val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", "dev163,dev164,dev165")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  val table = new HTable(conf, "usr_c_net_bhvr")

  //

  def main(args: Array[String]): Unit = {
    parseCURLToHbase("20170329")
    //parseCURLToHbase(args(0))
  }

  /**
    * 读取hdfs上的数据,将其处理后保存到hbase
    * @param date 日期类型 如:20170328
    */
  def parseCURLToHbase(date: String)={
    /**
      * 1.将HDFS上的文件,映射成表，pvTable，然后针对pvTable操作
      */
    val conf = new SparkConf().setAppName("ReadCURLToHbase").setMaster("local").set("spark.debug.maxToStringFields", "100")
    val sc = new SparkContext(conf)
    //val pvRDD = sc.textFile("hdfs://dev160:9000/hdfslvcheng/hdfslvcheng/lvcheng/20170328/phone_label_20170328.txt")
    val pvRDD = sc.textFile("hdfs://dev160:9000/hdfslvcheng/hdfslvcheng/lvcheng/"+ date + "/phone_label_" + date+ ".txt")
    val sqlContext = new SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame
    val schemaArray = schemaString.split(",")
    val schema = StructType(schemaArray.map(filedName => StructField(filedName, StringType, true)))

    val rowRDD = pvRDD
      .map(_.split("\t"))
      .map(eachRow => Row(eachRow(0), eachRow(1), eachRow(2), eachRow(3), eachRow(4), eachRow(5), eachRow(6), eachRow(7), eachRow(8)
        , eachRow(9), eachRow(10), eachRow(11), eachRow(12), eachRow(13), eachRow(14), eachRow(15), eachRow(16), eachRow(17)
        , eachRow(18), eachRow(19), eachRow(2), eachRow(21), eachRow(22)))

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
    // 遍历手机号,根据手机号查询每个用户的浏览url
    for (tel <- arrTel) {
      // 将一级标签,总pv, 存入sqlLab1(HashMap)集合中
      val sql = "SELECT url, COUNT(url), SUM(CAST(pv AS int)) AS totalPV " +
        "FROM " +
        "(SELECT SUBSTRING(curl,0,5) url,pv pv FROM pvTable WHERE tel='" + tel + "') " +
        "GROUP BY url"
      val sqlLab1 = sqlContext.sql(sql)
        .foreach { rows =>
          mapLab1 += (rows(0).toString -> rows(2).toString)
        }
      // 查询curl,pv
      val sqlStr1 = "SELECT curl,pv " +
        "FROM pvTable " +
        "WHERE tel='" + tel + "'"

      // 生成时间
      val now = new Date()
      val timestamp: Long = now.getTime

      // 遍历map集合mapLab1,依次遍历map中的一级标签,将其与每个手机号对应的每条记录进行比较,
      // 如果mapLab1的key和rows(0).toString.substring(0, 5)相等,
      // 则将其放入json中,如{"4":{"CU017002001":"3","CU017002002":"1"}}
      for (x <- mapLab1) {
        nodee = factory.objectNode()
        node1 = factory.objectNode()
        // 循环完得到完整的json
        sqlContext.sql(sqlStr1).foreach(rows => {
          // 将一级标签相同的url封装成json
          // 如{"4":{"CU017002001":"3","CU017002002":"1"}}
          if (rows(0).toString.substring(0, 5).equals(x._1.toString)) {
            node1.put(rows(0).toString, rows(1).toString)
            nodee.set(x._2.toString, node1)
          }
        }) //得到每个一级标签的每个手机号对应的json

        println(timestamp)
        // 生成rowkey, 随机数-(Long.MaxValue-timestamp)-0-客户区分标识-date
        val rowkey = Math.abs(java.lang.Long.hashCode(Long.MaxValue-timestamp)) % 3 + (Long.MaxValue-timestamp).toString + "0" + "a12" + date
        put = new Put(Bytes.toBytes(rowkey))
        // put值,手机号,
        put.addColumn(Bytes.toBytes("bhvr"), Bytes.toBytes("encryption_tel"), Bytes.toBytes(tel.toString))
        // put值,浏览记录,
        put.addColumn(Bytes.toBytes("bhvr"), Bytes.toBytes(ConstantUtils.parseLabToString(x._1).toString), Bytes.toBytes(nodee.toString))
        table.put(put)
        table.flushCommits()
      }
      // 清空HashMap
      mapLab1.clear()
    }
    // 清空ArrayBuffer
    arrTel.clear()
    // 关闭资源
    sc.stop()

  }
}
