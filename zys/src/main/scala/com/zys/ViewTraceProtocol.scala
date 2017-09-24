package com.zys

import java.util.Calendar
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import spray.json._

import scala.collection.mutable

case class ViewTrace(user: String, action: String, productId: String, productType: String)
object ViewTraceProtocol extends DefaultJsonProtocol{


  def main(args: Array[String]){

    implicit val viewTraceFormat = jsonFormat4(ViewTrace)
   // StreamingLogger.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("Streaming").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint("E:\\SparkLearn\\zys\\src\\main\\resources\\checkpoint")
    val rddQueue = new mutable.SynchronizedQueue[RDD[String]]()
    val stream = ssc.queueStream(rddQueue)
    import ViewTraceProtocol._
    val traces = stream.map(_.parseJson.convertTo[ViewTrace])
    val pv = traces.map(x => (x.productType, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(5), Seconds(2), 1).map(
      x => (Calendar.getInstance().getTime.toString, x._1, "PV", x._2)
    )
    pv.foreachRDD(rdd => rdd.foreachPartition(partitionOfRecords => {
      partitionOfRecords.foreach(record => if (record._4 > 0) {
        println(record.toString())
      })
    }))
    ssc.start()
    val s1 = """{"user":"Tom","action":"view","productId":"20","productType":"Covers"}"""
    val s2 = """{"user":"Groot","action":"view","productId":"14","productType":"Covers"}"""
    val s3 = """{"user":"Rocket","action":"view","productId":"13","productType":"Covers"}"""
    //Create and push some Rdds into
    val lines = List(s1, s2, s3)
    for (i <- 1 to 20) {
      rddQueue += ssc.sparkContext.makeRDD(lines, 2)
      Thread.sleep(1000)
    }

    Thread.sleep(6000)
    ssc.stop()
    ssc.awaitTermination()
  }

}
