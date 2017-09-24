package com.zys


import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.types.{DataTypes, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

case class Person(name:String, age:Int)

object createDF {
  def main(args: Array[String]){
    //Spark conf配置对象
    val conf = new SparkConf().setAppName("createDF").setMaster("local")
    val sc = new SparkContext(conf)
    //Spark SQL的入口
    val sQLContext = new SQLContext(sc)
    import sQLContext.implicits._
    //一、DataFrame的创建
    //1.Json文件

   // val df = sQLContext.read.json("E:\\SparkLearn\\zys\\src\\main\\resources")
    //2.parquent文件创建

   // val df = sQLContext.read.parquet("E:\\SparkLearn\\zys\\src\\main\\resources\\users.parquet")

    //3.avro文件创建

    //val df = sQLContext.read.avro()
    //4.通过RDD方式创建
      //反射方式创建DataFrame
    //var rdd = sc.textFile("E:\\SparkLearn\\zys\\src\\main\\resources\\people1").map(_.split(",")).map(p=>Person(p(0), p(1).trim.toInt))
    //val df = rdd.toDF()
    //df.registerTempTable("peopletable")

    //val result = sQLContext.sql("SELECT name, age FROM peopletable WHERE age >= 13 AND age <=19")
    //result.map(t=>"Name:"+t(0)).collect().foreach(println)
   // result.map(t=>"Name:"+t.getAs[String]("name")).collect().foreach(println)

    //注册元素数据方法

   // var rdd = sc.textFile("E:\\SparkLearn\\zys\\src\\main\\resources\\people1")
    //            .map{ line =>
      //            val strs = line.split(",")
        //          Row(strs(0), strs(1).trim.toInt)
          //      }
   // var structType = StructType(Array(
     // StructField("name", DataTypes.StringType),
      //StructField("age", DataTypes.IntegerType)
    //))

    //以编程方式定义RDD模式
    val rdd = sc.textFile("E:\\SparkLearn\\zys\\src\\main\\resources\\people1")
    val schemaString = "name age"
    val schema =StructType(
      schemaString.split(" ").map(fieldName => StructField(fieldName, DataTypes.StringType, true))
    )
    val rowRDD = rdd.map(_.split(",")).map(p=> Row(p(0), p(1).trim))

    val df = sQLContext.createDataFrame(rowRDD, schema)
    //打印对应的约束信息
    //df.printSchema()
    //小数据量时候，客户端显示数据
    //df.show()

    //API collect()操作

    //val arrs = df.collect()
   // for(ele <- arrs){
   //   println(ele)
   // }

    //API collectAsList()操作

   // val list = df.collectAsList()
   // for(i <- 0 until list.size()){
   //   println(list.get(i))
   // }

    //API count()操作

//    println(df.count())

    //API describe()操作

//    println(df.describe("name","age"))
    //API head()操作
//    println(df.first())
//    for(ele<-df.head(2)){
//      println(ele)
//    }
    //API take()取数操作
//    for(ele<-df.take(1)){
//      println(ele)
//    }

    //API columns操作
//      for(ele<-df.columns){
//        println(ele)
//      }
//    println(df.schema)

  //DataFrame查询操作
//    println(df.select("age").first())
    //条件过滤查寻
//    println(df.filter(df.col("age").gt(20)))
//    println(df.filter(df.col("age").gt(20)).first())

  //API聚合操作
//    println(df.agg("name" ->"count").first())
  //API分组操作
//  println(df.groupBy("name").count())

  }
}
