package com.zys

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object OpHiveSql {
  def main(args: Array[String]) {

    //spark入口
    //spark配置对象
    val conf = new SparkConf().setAppName("OpHiveSql").setMaster("local")
    val sc = new SparkContext(conf)

    //创建HiveContext文件
    val hiveContext = new HiveContext(sc)

    hiveContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    hiveContext.sql("LOAD DATA LOCAL INPATH  '/usr/kv1.txt' INTO TABLE src")

    //Queries are expressed in HiveQL
    hiveContext.sql("FROM src SELECT key, value").collect().foreach(println)

  }
}
