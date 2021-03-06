package greentown.lc.test

import org.apache.log4j.{BasicConfigurator, Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by saicao on 2017/7/20.
  */
object ScalaTest6 {

  val arrAD = ArrayBuffer[String]()
  private val schemaString = "accs_nbr,label_id,type,flag,load_date,load_date1"

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ScalaTest6").setMaster("local")//.set("spark.debug.maxToStringFields", "100")
    val sc = new SparkContext(conf)
    val pvRDD: RDD[String] = sc.textFile("file:///C:\\Users\\saicao\\Desktop\\c_test.txt")
    val sqlContext = new SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame
    val schemaArray = schemaString.split(",")
    val schema = StructType(schemaArray.map(filedName => StructField(filedName, StringType, true)))
    val rowRDD = pvRDD
      .map(_.split("\t"))
      .map(eachRow => Row(eachRow(0), eachRow(1), eachRow(2), eachRow(3), eachRow(4), eachRow(5)))

    //var pvDF1:DataFrame=_

    val pvDF: DataFrame = sqlContext.createDataFrame(rowRDD, schema)
    pvDF.registerTempTable("pvTable")

    //pvDF1 = sqlContext.createDataFrame(rowRDD, schema)

    val sql = "select * from pvTable"
    //pvDF1.show()
    val sss: DataFrame = sqlContext.sql(sql)
    sss.show()


    val bb: RDD[Unit] = pvDF.select("accs_nbr").distinct().map { x => {
      val x1: Any = x.get(0)
      //println(x1)
      println("444444222222" + x)
      val sql = "select * from pvTable where accs_nbr=" + x
      //pvDF1.show()
      val sss: DataFrame = sqlContext.sql(sql)
      sss.show()
    }
    }

    bb.foreach(x=>println(x))
  }
}
