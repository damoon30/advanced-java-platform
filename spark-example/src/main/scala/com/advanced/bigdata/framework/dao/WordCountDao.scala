package com.advanced.bigdata.framework.dao

import com.advanced.bigdata.framework.unit.EnvUtil
import org.apache.spark.rdd.RDD

/**
 * 控制层
 */
class WordCountDao {


  def readFile(file:String): RDD[String] = {
    EnvUtil.take().read.textFile(file).rdd
  }
}
