package com.advanced.bigdata.rdd

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object Spark02_Rdd_Create {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder.appName("RDD").master("local").getOrCreate()
    val sc : SparkContext = spark.sparkContext

    val rdd1: RDD[Int] = sc.parallelize(List(11, 2, 3, 4))

    //collect 是采集到Driver
    rdd1.collect().foreach(println)


    val rdd2:RDD[Int] = sc.makeRDD(Seq(1, 2, 3, 4))
    rdd2.collect().foreach(println)


    // 从文件中创建
    val fileRdd3:RDD[String] = spark.read.textFile("datas/data.txt").rdd
    fileRdd3.collect().foreach(println)


    // 从文件中创建
    val rdd4 : RDD[String] = sc.textFile("datas/data.txt")
    rdd4.collect().foreach(println)

    // 从文件中创建
    val rdd5 : RDD[(String,String)] = sc.wholeTextFiles("datas")
    rdd5.collect().foreach(println)

    spark.stop()
  }
}
