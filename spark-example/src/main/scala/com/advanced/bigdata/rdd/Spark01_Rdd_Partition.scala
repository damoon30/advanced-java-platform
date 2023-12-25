package com.advanced.bigdata.rdd

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
 * spark 分区
 */
object Spark01_Rdd_Partition {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder.appName("RDD").master("local").getOrCreate()

    val sc : SparkContext = spark.sparkContext

    //Rdd的分区和并行度
    // makeRDD 第二个参数时并行度，默认

    // 数据时如何分配到不同的分区的呢，划分代码

    // 算数组的下标
    //def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
    //      (0 until numSlices).iterator.map { i =>
    //        val start = ((i * length) / numSlices).toInt
    //        val end = (((i + 1) * length) / numSlices).toInt
    //        (start, end)
    //      }
    //    }
    // slice=0时，获取数组开始下标 （0,1） -->对应数据1
    // slice=1时，获取数组开始下标 （1,3） --> 对应数据2，3
    // slice=3时，获取数组开始下标 （3,5） --> 对应数据4，5

    // 根据数组的下标来获取元素
    val rdd : RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 5)
//    val rdd1 : RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    positions(4, 5).foreach(ot => println("数组开始下标："+ot._1, "数组结束下标"+ot._2))

    rdd.saveAsTextFile("datas/partition3")
//    rdd1.saveAsTextFile("datas/partitionDefault")

    spark.stop()
  }

  def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
        (0 until numSlices).iterator.map { i =>
          val start = ((i * length) / numSlices).toInt
          val end = (((i + 1) * length) / numSlices).toInt
          (start, end)
        }
      }


}
