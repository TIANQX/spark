package com.zd.bigdata.spark.core.rdd.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_Test {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "F:\\BigDataProgramFile\\winutils")
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)
    //转换算子
    val rdd = sparkContext.textFile("datas/apache.log")

    val mapRdd = rdd.map(
      line => {
        val datas = line.split(" ")
        datas(0)
      }
    )
    mapRdd.collect().foreach(println)
    sparkContext.stop()
  }
}
