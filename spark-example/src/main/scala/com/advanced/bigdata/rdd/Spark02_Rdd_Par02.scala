package com.advanced.bigdata.rdd

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Spark02_Rdd_Par02 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder.appName("RDD").master("local").getOrCreate()
    val sc : SparkContext = spark.sparkContext

    // 从文件中读取，分区和并行度
    // 数据以行读取
    // spark读取文件，采用的是hadoop的方式读取，是一行一行读取的，和字节数没关系
    // 数据读取时以偏移量为单位, 偏移量不会被重复读取
    /**
     * 1@
     * 2@
     * 3@
     * 4@
     * 5@
     * 6@
     * 7
     */

    /**
     * 3个分区，分别对应偏移量，13个字节 13/2=6字节
     * 0==> [0,6]
     * 1==> [6,12]
     * 2 ==>[12,18]
     */
    val rdd: RDD[String] = sc.textFile("datas/1.txt", 2)
    // 分区数量的计算方式
    rdd.saveAsTextFile("datas/test01")

    spark.stop()
  }
}
