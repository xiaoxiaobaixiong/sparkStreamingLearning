package com.zys

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StatefulNetworkWordCount {

  def main(args: Array[String]): Unit = {
    if (args.length < 2){
      System.err.println("Usage: StatefulNetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum
      val previousCount = state.getOrElse(0)

      Some(currentCount + previousCount)
    }

    val newUpdateFunc = (iterator: Iterator[(String, Seq[Int], Option[Int])]) => {
      iterator.flatMap(t => updateFunc(t._2, t._3).map(s => (t._1, s)))
    }

    val sparkConf = new SparkConf().setAppName("StatefulNetworkWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint(".")

    val lines = ssc.socketTextStream(args(0), args(1).toInt)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1))

    val stateDstream = wordCounts.updateStateByKey[Int](newUpdateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
    stateDstream.print()

    ssc.start()
    ssc.awaitTermination()
  }



}
