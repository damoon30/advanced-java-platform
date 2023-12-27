package com.advanced.bigdata.df

import com.advanced.bigdata.CommonApp
import com.advanced.bigdata.framework.unit.EnvUtil
import org.apache.spark.sql.DataFrame

object Spark_Df_01 extends App with CommonApp{

  // 直接写逻辑
  start() {
    val spark = EnvUtil.take()

    val iDf:DataFrame = spark.read.json("business/impression.json")
    // 获取iDf内容
    iDf.printSchema()
//    iDf.show()




    val cDf:DataFrame = spark.read.json("business/click.txt")
    cDf.printSchema()
//    cDf.show()

    // 左链接
    val join = iDf.join(cDf, "pin", "left")
//    join.show()

    val joinExpression = iDf.col("pin") === cDf.col("pin") and iDf.col("dt") === cDf.col("dt")
    val join1 = iDf.join(cDf, joinExpression, "left")

//    join1.printSchema()
//    iDf.joinWith()
    val join2 = iDf.join(cDf,  Seq("pin", "dt"), "left")
    join2.printSchema()
    join2.show()
  }
}
