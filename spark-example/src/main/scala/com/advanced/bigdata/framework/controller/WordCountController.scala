package com.advanced.bigdata.framework.controller

import com.advanced.bigdata.framework.service.WordCountService
import org.apache.spark.rdd.RDD

/**
 * 控制层
 */
class WordCountController {

  private val ws = new WordCountService;

  def dispatch(): Unit = {
    val tuples: Array[(String, Int)] = ws.dataAnalysis()

    //打印
    for (elem <- tuples) {
      println(elem)
    }
  }
}
