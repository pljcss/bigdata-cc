package greentown.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by saicao on 2017/8/27.
  */
object WordCountStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setMaster("local[2]")
          .setAppName("WordCountStreaming")

    val ssc = new StreamingContext(conf,Seconds(1))
    // 首先创建输入DStreaming,代表一个从源数据(比如kafka,socket)来的持续不断的实时数据流
    // socketTextStream方法,可以创建一个数据源为socket网络端口的数据流,
    // 其接收两个参数,第一个代表那个主机上的端口,第二个表表哪个端口
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",4444)
    // 到这里为止,可以理解为ReceiverInputDStream中的,每隔一秒,会有一个RDD,其中封装了这
    // 一秒发过来的数据,RDD的元素类型为string,即一行一行的文本


    // 总结
    /**
      * 每秒中发送到指定端口上的数据,都会被lines DStream接收到
      * 然后lines DStream会把每秒的数据,也就是一行行的文本,诸如hell world封装为一个RDD
      * 然后,就会对每秒中对应的RDD,执行后续的一系类的算子操作
      * 比如,对lines RDD执行了flatMap之后,就得到一个word RDD,作为words DStream中的一个RDD
      * 以此类推
      * 但是,一定要注意,Spark Streaming的计算模型,就决定了,我们必须自己来进行中间缓存的控制
      * 比如写入redis等缓存
      * 它的计算模型跟Storm是完全不同的,storm是自己编写的一个个程序,运行在节点上,相当于一个一个
      * 的对象,可以自己在对象中控制缓存
      * 但是Spark本身是函数式编程的计算模型,所以,比如在words或pairs DStream中,没法在实例变量中进行缓存
      * 此时就只能将最后计算出的wordCounts中的一个一个的RDD,写入外部的缓存,或者持久化DB
      */


    // 此处不能是map
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word=>(word,1))
    val wordCounts = pairs.reduceByKey(_ + _)
    // 程序并未使其休眠
    Thread.sleep(5000)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()




  }

}
