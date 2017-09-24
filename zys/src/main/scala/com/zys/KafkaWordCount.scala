package com.zys

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{ Seconds, StreamingContext}

object KafkaWordCount {
  def main(args: Array[String]): Unit = {

    if (args.length < 4){
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topic> <numThreads>")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint(".")

    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordcounts = words.map(x => (x, 1L)).reduceByKeyAndWindow(_+_,_-_,Seconds(10), Seconds(2), 2)
    wordcounts.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
