package greentown.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 基于updateStateByKey算子实现缓存机制的实时wordcount程序
  * Created by saicao on 2017/8/27.
  */
object UpdateStateByKeyWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UpdateStateByKeyWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    // 第一点，如果要使用updateStateByKey算子，就必须设置一个checkpoint目录，开启checkpoint机制
    // 这样的话才能把每个key对应的state除了在内存中有，那么是不是也要checkpoint一份
    // 因为你要长期保存一份key的state的话，那么spark streaming是要求必须用checkpoint的，以便于在
    // 内存数据丢失的时候，可以从checkpoint中恢复

    // 开启checkpoint，很简单，只要调用ssc的checkpoint()方法,设置一个hdfs目录即可
    ssc.checkpoint("hdfs://greentown/wordcount_checkpoint")

    // 然后先实现基础的wordcount逻辑
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",4444)

    // 此处不能是map
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word=>(word,1))
    // val wordCounts = pairs.reduceByKey(_ + _)
    // wordCounts.print()
    // 到了这里，就不一样了，之前的话，是不是直接就是pairs.reduceByKey
    // 然后，就可以得到每个时间段的batch对应的RDD，计算出来的单词统计
    // 然后，可以打印出那个时间段的单词统计
    // 但是，如果要统计每个单词出现的全局的计算呢？
    // 就是说，统计出来，从程序启动开始，到现在为止，一个单词出现的次数，那么就之前的方式就不好实现了
    // 就必须基于redis这样的缓存，或者mysql这种db，来实现累加

    // 但是，我们的updateStateByKey，就可以实现直接通过spark维护一份每个单词的全局统计次数
    val wordCounts = pairs.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
      var newValue = state.getOrElse(0)
      for(value <- values) {
        newValue += value
      }
      Option(newValue)
    })

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
