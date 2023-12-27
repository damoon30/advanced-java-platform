package com.advanced.bigdata

import com.advanced.bigdata.framework.unit.EnvUtil
import org.apache.spark.sql.SparkSession

trait CommonApp {

  def start()(op : => Unit ): Unit = {
    val spark = SparkSession.builder().appName("testSpark").master("local[*]").getOrCreate()
    EnvUtil.put(spark)

    try {
      op
    } catch {
      case ex => println(ex)
    }
    spark.stop()
  }
}
