package greentown.lc.test

import java.util.Date

import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by saicao on 2017/7/19.
  */
object ToHbaseTest3 {

  var timestamp:Long = 0
  var now:Date = null
  var t1:Long=0

  // scala中声明全局变量在main方法外
  // 将手机号封装在ArrayBuffer中
  val arrAD = ArrayBuffer[String]()
  val mapLab1 = new scala.collection.mutable.HashMap[String, String]

  //val fileWriter = new FileWriter("C:\\Users\\saicao\\Desktop\\res_c_test.txt")
  //var out:PrintWriter = new PrintWriter(fileWriter)

  private val schemaString = "accs_nbr,label_id,type,flag,load_date,load_date2"

  // JackSon
  val factory = new JsonNodeFactory(false)
  var node1: ObjectNode = null
  var nodee: ObjectNode = null


  /*
  val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", "gt79,gt91,gt44")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  val table = new HTable(conf, "test_c_label")
*/

  // rowkey
  var rowkey:String = null

  // 存储TSV
  var finance:String = null
  var sport:String = null
  var science:String = null
  var life:String = null
  var entertainment:String = null
  var education:String = null
  var political:String = null
  var shopping:String = null
  var reading:String = null
  var video:String = null
  var music:String = null
  var game:String = null
  var portal:String = null
  var illegal_site:String = null
  var region:String = null
  var access_way:String = null
  var others:String = null

  def main(args: Array[String]): Unit = {
    parseCURLToHbase("20173333")
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
    val conf = new SparkConf().setAppName("ReadCURLToHbase").setMaster("local")//.set("spark.debug.maxToStringFields", "100")
    val sc = new SparkContext(conf)
    //val pvRDD = sc.textFile("hdfs://dev160:9000/hdfslvcheng/hdfslvcheng/lvcheng/20170328/phone_label_20170328.txt")
    //val pvRDD = sc.textFile("hdfs://nameservice1/lcdmp/new_l_view_st_c/20170610")

    // 测试_读取本地文件
    //val pvRDD = sc.textFile("file:///C:\\Users\\saicao\\Desktop\\c_test.txt")

    //val pvRDD = sc.textFile("hdfs://nameservice1/lcdmp/new_l_view_st_c/20170610/000000_0")
    //val pvRDD = sc.textFile("hdfs://nameservice1/lcdmp/new_l_view_st_c/20170610/*")
    val pvRDD = sc.textFile("file:///C:\\Users\\saicao\\Desktop\\c_test.txt")

    val sqlContext = new SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame
    val schemaArray = schemaString.split(",")
    val schema = StructType(schemaArray.map(filedName => StructField(filedName, StringType, true)))

    val rowRDD = pvRDD
      .map(_.split("\t"))
      .map(eachRow => Row(eachRow(0), eachRow(1), eachRow(2), eachRow(3), eachRow(4), eachRow(5)))

    //val pvDF = sqlContext.createDataFrame(rowRDD, schema)
    val pvDF = sqlContext.createDataFrame(rowRDD, schema)
    // 注册为临时表
    pvDF.registerTempTable("pvTable")


    /**
      * 修改后
      */
    // HBase配置信息
    val tablename = "test_c_label"
    sc.hadoopConfiguration.set("hbase.zookeeper.quorum","gt79,gt91,gt44")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    val job = new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    var put:Put = null


    /**
      * 具体操作
      * 全局变量的声明
      */
    // 查询有手机号(并去重),并将手机号存放到ArrayBuffer
    val adDF: DataFrame = sqlContext.sql("SELECT DISTINCT accs_nbr FROM pvTable")

    // 将手机号存入arrTel
    adDF.foreach { ad => {
      //println(tels(0))
      arrAD += ad(0).toString
      //println(arr(0))
      }
    }
    println("11144444444" + arrAD.size)

    val rddd: RDD[String] = sc.makeRDD(arrAD)
    //println("111114444445555555" + rddd)
    rddd.foreach{x=>println("11144446666" + x)}
    // 遍历手机号,根据手机号查询每个用户的浏览url





    //val rr: Unit = for (tel <- arrAD) {
    val rdd: RDD[(ImmutableBytesWritable, Put)] = rddd.map { tel => {
      println("111111155555551111111" + tel)

      // 一级标签对应的名称
      val allLabMap = mutable.HashMap[String, String]("CU001" -> "finance", "CU002" -> "sport", "CU003" -> "science",
        "CU004" -> "life", "CU005" -> "entertainment", "CU006" -> "education", "CU007" -> "political",
        "CU008" -> "shopping", "CU009" -> "reading", "CU010" -> "video", "CU011" -> "music", "CU012" -> "game",
        "CU013" -> "portal", "CU014" -> "illegal_site", "CU015" -> "region", "CU016" -> "access_way", "CU017" -> "others")


      // accs_nbr,label_id,type,flag,load_date
      // 将一级标签,总pv, 存入sqlLab1(HashMap)集合中
      //val sql = "select substring(label_id,0,5),count(substring(label_id,0,5)) from pvTable where flag=4 and accs_nbr=" + tel + " group by substring(label_id,0,5)"
      //println(tel)

      //val sql = "select substring(label_id,0,5),count(substring(label_id,0,5)) from pvTable where accs_nbr=" + "'" + tel.toString + "' group by substring(label_id,0,5)"
      val sql = "select substring(label_id,0,5),count(substring(label_id,0,5)) from pvTable where accs_nbr=" + "'" + "495f08a3bf35e6fb14e17982db6f3fa8" + "' group by substring(label_id,0,5)"

      println("11111117777770000000")

      sqlContext.sql(sql).show()

      val sqlLab1 = sqlContext.sql(sql)
        .foreach { rows =>
          mapLab1 += (rows(0).toString -> rows(1).toString)
        }


      println("11111777777777777777777777")

      // 查询 label_id, pv
      val sqlStr1 = "SELECT label_id,count(label_id) " +
        "FROM pvTable " +
        "WHERE flag=4 and accs_nbr=" + tel + " GROUP BY label_id"

      val sqlStr2 = "SELECT label_id,count(label_id) " +
        "FROM pvTable " +
        "WHERE accs_nbr=" + "'" + tel + "' GROUP BY label_id"


      // 遍历map集合mapLab1,依次遍历map中的一级标签,将其与每个手机号对应的每条记录进行比较,
      // 如果mapLab1的key和rows(0).toString.substring(0, 5)相等,
      // 则将其放入json中,如{"4":{"CU017002001":"3","CU017002002":"1"}}
      for (x <- mapLab1) {
        nodee = factory.objectNode()
        node1 = factory.objectNode()
        // 循环完得到完整的json
        sqlContext.sql(sqlStr2).foreach(rows => {
          // 将一级标签相同的url封装成json
          // 如{"4":{"CU017002001":"3","CU017002002":"1"}}
          if (rows(0).toString.substring(0, 5).equals(x._1.toString)) {
            node1.put(rows(0).toString, rows(1).toString)
            nodee.set(x._2.toString, node1)
          }
        }) //得到每个一级标签的每个手机号对应的json


        // 模式匹配
        x._1.toString match {
          case "CU001" => finance = nodee.toString
          case "CU002" => sport = nodee.toString
          case "CU003" => science = nodee.toString
          case "CU004" => life = nodee.toString
          case "CU005" => entertainment = nodee.toString
          case "CU006" => education = nodee.toString
          case "CU007" => political = nodee.toString
          case "CU008" => shopping = nodee.toString
          case "CU009" => reading = nodee.toString
          case "CU010" => video = nodee.toString
          case "CU011" => music = nodee.toString
          case "CU012" => game = nodee.toString
          case "CU013" => portal = nodee.toString
          case "CU014" => illegal_site = nodee.toString
          case "CU015" => region = nodee.toString
          case "CU016" => access_way = nodee.toString
          case "CU017" => others = nodee.toString
        }
      }

      //println("111111144444411111:::::" + allLabMap.size)
      for (xx <- mapLab1) {
        allLabMap.remove(xx._1)
      }

      //println("11111144444455555:::::" + allLabMap.size)
      for (x <- allLabMap) {

        x._1.toString match {
          case "CU001" => finance = "NULL"
          case "CU002" => sport = "NULL"
          case "CU003" => science = "NULL"
          case "CU004" => life = "NULL"
          case "CU005" => entertainment = "NULL"
          case "CU006" => education = "NULL"
          case "CU007" => political = "NULL"
          case "CU008" => shopping = "NULL"
          case "CU009" => reading = "NULL"
          case "CU010" => video = "NULL"
          case "CU011" => music = "NULL"
          case "CU012" => game = "NULL"
          case "CU013" => portal = "NULL"
          case "CU014" => illegal_site = "NULL"
          case "CU015" => region = "NULL"
          case "CU016" => access_way = "NULL"
          case "CU017" => others = "NULL"
        }

      }
      /*
            out.write(rowkey + "\t" + tel + "\t" + finance + "\t" + sport + "\t" + science + "\t" + life + "\t" + entertainment
              + "\t" + education + "\t" + political + "\t" + shopping + "\t" + reading + "\t" + video + "\t" + music + "\t" + game
              + "\t" + portal + "\t" + illegal_site + "\t" + region + "\t" + access_way + "\t" + others)
            out.println()
      */

      now = new Date()
      timestamp = now.getTime

      while (timestamp == t1) {
        timestamp = new Date().getTime
      }
      /*
      // 生成时间
      val now = new Date()
      val timestamp: Long = now.getTime
      rowkey = Math.abs(java.lang.Long.hashCode(Long.MaxValue - timestamp)) % 3 + (Long.MaxValue - timestamp).toString + "0" + "a12" + date
*/
      val rowkey = Math.abs(java.lang.Long.hashCode(Long.MaxValue - timestamp)) % 3 + tel.toString + "0" + "a11" + date

      println("22277775555" + rowkey)
      // 写入HBase
      put = new Put(Bytes.toBytes(rowkey))
      // put值,手机号,
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("encryption_tel"), Bytes.toBytes(tel.toString))
      // put值,浏览记录,通信行为
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("finance"), Bytes.toBytes(finance))
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("sport"), Bytes.toBytes(sport))
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("science"), Bytes.toBytes(science))
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("life"), Bytes.toBytes(life))
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("entertainment"), Bytes.toBytes(entertainment))
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("political"), Bytes.toBytes(political))
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("shopping"), Bytes.toBytes(shopping))
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("reading"), Bytes.toBytes(reading))
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("video"), Bytes.toBytes(video))
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("music"), Bytes.toBytes(music))
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("game"), Bytes.toBytes(game))
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("portal"), Bytes.toBytes(portal))
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("illegal_site"), Bytes.toBytes(illegal_site))
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("region"), Bytes.toBytes(region))
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("access_way"), Bytes.toBytes(access_way))
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("others"), Bytes.toBytes(others))

      //table.put(put)
      //table.flushCommits()

      t1 = timestamp

      finance = null
      sport = null
      science = null
      life = null
      entertainment = null
      education = null
      political = null
      shopping = null
      reading = null
      video = null
      music = null
      game = null
      portal = null
      illegal_site = null
      region = null
      access_way = null
      others = null


      // 清空HashMap
      mapLab1.clear()
      allLabMap.clear()
      (new ImmutableBytesWritable, put)
    }
    }
      // 清空ArrayBuffer
      arrAD.clear()
      // 关闭资源

      //fileWriter.close()
      //out.close()
      rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())

      sc.stop()

  }

}
