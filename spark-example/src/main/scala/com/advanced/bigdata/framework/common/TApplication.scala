package com.advanced.bigdata.framework.common

import com.advanced.bigdata.framework.controller.WordCountController
import com.advanced.bigdata.framework.unit.EnvUtil
import org.apache.spark.sql.SparkSession

trait TApplication {

  def start(master:String="local", app:String="application")(op : => Unit ): Unit ={
    val spark = SparkSession.builder.appName(app).master(master).getOrCreate()
    EnvUtil.put(spark)
    try {
      op
    } catch {
      case ex => println(ex)
    }

    spark.stop()
  }
}
