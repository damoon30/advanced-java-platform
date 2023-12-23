package com.advanced.bigdata.framework.unit

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object EnvUtil {

  val scLocal = new ThreadLocal[SparkSession]()

  def put(sc: SparkSession): Unit = {
    scLocal.set(sc)
  }


  def take(): SparkSession = {
    scLocal.get()
  }


  def clear(): Unit = {
    scLocal.remove()
  }

}
