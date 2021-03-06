package greentown.lc.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by saicao on 2017/5/24.
  */
object ScalaTest1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ScalaTest1").setMaster("local")
    val sc = new SparkContext(conf)


    val loupanRDD = sc.textFile("file:///C:\\Users\\saicao\\Desktop\\jj3\\c_view_st_20170510.txt.gz")


    val res = loupanRDD.take(10000).foreach(x=>println(x))
    //println(res)

    //println(res)


  }




}
