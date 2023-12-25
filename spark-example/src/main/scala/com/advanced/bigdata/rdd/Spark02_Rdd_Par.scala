package com.advanced.bigdata.rdd

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.io.File

object Spark02_Rdd_Par {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder.appName("RDD").master("local").getOrCreate()
    val sc : SparkContext = spark.sparkContext

    //1.txt 数据只有
    // 1@
    // 2@
    // 3

    // 从文件中读取，分区和并行度
    // minPartitions  math.min(defaultParallelism, 2), 实际上是3个分区   1.txt是7个字节
    //long goalSize = file.gteLen(字节数) / (numSplits == 0 ? 1 : numSplits);  ---> 5/2=2， 也就是每个文件的大小数2kb，7kb一共需要3个文件，所以partition=3
    val rdd: RDD[String] = sc.textFile("datas/1.txt", 2)

    // 分区数量的计算方式
    rdd.saveAsTextFile("datas/test04")


    // long goalSize = file.gteLen(字节数) / (numSplits == 0 ? 1 : numSplits);  ---> 35/2=17余1， 也就是每个文件的大小数17kb，35kb一共需要3个文件，所以partition=3
    val rdd1: RDD[String] = sc.textFile("datas/data.txt", 2)

    // 分区数量的计算方式
    rdd1.saveAsTextFile("datas/test05")

    spark.stop()
  }
}
