package greentown.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by saicao on 2017/8/27.
  */
object HDFSWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HDFSWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))



    val lines = ssc.textFileStream("hdfs://greentown/wordcount_dir")
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word=>(word,1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()




  }
}
