package com.zys

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object WindowsWordCount {
  def main(args: Array[String]): Unit = {

    if (args.length < 2){
      System.err.println("Usage: WindowsWordCount <hostname> <port>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("WindowsWordCount")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint(".")

    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_ONLY_SER)
    val words = lines.flatMap(_.split(","))

    val wordCounts = words.map(x =>(x, 1)).reduceByKeyAndWindow((a: Int, b: Int)=>(a+b), Seconds(args(2).toInt), Seconds(args(3).toInt))
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
