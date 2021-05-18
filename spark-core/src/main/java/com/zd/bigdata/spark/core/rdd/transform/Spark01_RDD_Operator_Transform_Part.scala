package com.zd.bigdata.spark.core.rdd.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_Part {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "F:\\BigDataProgramFile\\winutils")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    // TODO 算子 - map
    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4), 2)
    rdd.saveAsTextFile("output")

    var mapRDD = rdd.map(_ * 2)
    mapRDD.saveAsTextFile("output1")
    sparkContext.stop()
  }


}
