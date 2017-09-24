package com.zys

import org.apache.spark.{SparkConf, SparkContext}

object simpleapp {
  def main(args: Array[String]){
    val logFile = "E:\\SparkLearn\\zys\\src\\main\\resources\\word.txt"
    val conf = new SparkConf().setAppName("simpleapp").setMaster("spark://192.168.111.20:7077")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line =>line.contains("a")).count()
    val numBs = logData.filter(line =>line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
