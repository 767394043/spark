package cn.ffcs.mss.spark.test
//import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by Titanium on 2017/3/6.
  */
object KafkaSparkDemoMain {
  def main(args: Array[String]): Unit = {
//    println("=======================================================================")
//    val conf = new SparkConf().setAppName("KafkaStreaming").setMaster("local[2]")
//
//    val ssc = new StreamingContext(conf,Seconds(10))
//    val zkQuorum = "is-nn-01:9092,is-dn-01:9092,is-dn-02:9092,is-dn-03:9092,is-dn-04:9092/kafka"
//    val group = "kafka-streaming-group"
//    val topicMap = Map("ssTopic" -> 1)
//    val lines = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap).map(_._2)
//    lines.print()
//    val words = lines.flatMap(_.split(" "))
//    val wordCounts = words.map(x => (x,1L)).reduceByKey(_+_)
//    wordCounts.print()
//
//    ssc.start()
//    ssc.awaitTermination()
  }
}
