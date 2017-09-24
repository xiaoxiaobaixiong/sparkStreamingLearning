package com.zys

import org.apache.spark.SparkConf

import org.apache.spark.streaming.{Seconds, StreamingContext}

object AQuickExample {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("AQuickExample")
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word =>(word, 1))
    val wordCounts = pairs.reduceByKey(_+_)

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
