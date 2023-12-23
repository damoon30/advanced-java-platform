package com.advanced.bigdata.framework.service

import com.advanced.bigdata.framework.application.WordCountApplication.spark
import com.advanced.bigdata.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

/**
 * 控制层
 */
class WordCountService {

  private val wo = new WordCountDao()


  def dataAnalysis(): Array[(String, Int)] = {
    wo
    // 读取文件
    val lines: RDD[String] = wo.readFile("datas/data.txt")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))


    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)


    val tuples: Array[(String, Int)] = wordToSum.collect()
    tuples
  }


}
