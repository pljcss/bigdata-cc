package greentown.lc.test

import java.util.Date

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions



object ScalaSmartTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum","10.0.0.120,10.0.0.184,10.0.0.91")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    val tablename = "smartcommunity"

    //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    val indataRDD = sc.makeRDD(Array("添加门禁绑定,人脸识别,https://zos.alipayobjects.com/rmsportal/jkjgkEfvpUPVyRjUImniVslZfWPnJuuZ.png,1520946393857,18270809203,熊豪,租客","添加门禁绑定,人脸识别,https://zos.alipayobjects.com/rmsportal/jkjgkEfvpUPVyRjUImniVslZfWPnJuuZ.png,1520946393657,18270809203,熊豪,租客"
    ))


    val rdd = indataRDD.map(_.split(',')).map{arr=>{
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
      val now = new Date()
      val now_time = now.getTime
      val rowkey = Long.MaxValue-now_time
      val put = new Put(Bytes.toBytes(rowkey))

      put.add(Bytes.toBytes("cf"),Bytes.toBytes("entranceName"),Bytes.toBytes(arr(0)))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("openMethod"),Bytes.toBytes(arr(1)))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("portrait"),Bytes.toBytes(arr(2)))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("recognizeTime"),Bytes.toBytes(arr(3)))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("residentMobile"),Bytes.toBytes(arr(4)))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("residentName"),Bytes.toBytes(arr(5)))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("residentType"),Bytes.toBytes(arr(6)))
      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, put)
    }}

    rdd.saveAsHadoopDataset(jobConf)

    sc.stop()
  }

}
