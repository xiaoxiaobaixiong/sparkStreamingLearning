package com.zys

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

case class Person1(name: String, age: Long)
object DS {
  def main(args: Array[String]){


    //Spark conf配置对象
    val conf = new SparkConf().setAppName("createDF").setMaster("local")
    val sc = new SparkContext(conf)
    //Spark SQL的入口
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    //DataSet的创建
    //1.通过集合的方式
   // var ds = List(Person("Kevin", 24),Person("Jhon", 20)).toDS()
//    val list = List(Person1("Kevin", 24),Person1("Jhon", 20))
//    val frame = sqlContext.createDataFrame(list)
//    frame.printSchema()
    //2.通过RDD创建
//    var rdd = sc.textFile("E:\\SparkLearn\\zys\\src\\main\\resources\\people1")
//                  .map{line =>
//                    val strs = line.split(",")
//                    Person1(strs(0), strs(1).trim.toInt)
//                  }

//    var ds = rdd.toDS()
    //3.通过DataFrame创建
    var ds = sqlContext.read.json("E:\\SparkLearn\\zys\\src\\main\\resources\\people").as[Person1]
    ds.printSchema()
    ds.show()
    ds.filter(person => person.age > 21).show()
  }
}
