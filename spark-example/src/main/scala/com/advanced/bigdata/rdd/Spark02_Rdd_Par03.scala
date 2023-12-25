package com.advanced.bigdata.rdd

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Spark02_Rdd_Par03 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder.appName("RDD").master("local").getOrCreate()
    val sc : SparkContext = spark.sparkContext

    // 从文件中读取，分区和并行度
    // 数据以行读取
    // spark读取文件，采用的是hadoop的方式读取，是一行一行读取的，和字节数没关系
    // 数据读取时以偏移量为单位, 偏移量不会被重复读取
    /** 内容             偏移量
     * 1234567@  -->01234567
     * 89@  -->8910
     * 0  -->11
     */

    /**
     * 文件字节数=13，13/3=4字节，每个文件4字节，剩1字节，分别对应偏移量
     * 0==> [0,4]
     * 1==> [4,8]
     * 2==> [8,12]
     */
    val rdd: RDD[String] = sc.textFile("datas/data.txt", 3)
    // 分区数量的计算方式
    rdd.saveAsTextFile("datas/test02")

    spark.stop()
  }
}
