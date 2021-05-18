package com.zd.bigdata.spark.core.rdd.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "F:\\BigDataProgramFile\\winutils")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - mapPartitions
    //flatMap

    val rdd = sc.makeRDD(List(List(1, 2), List(3, 4)))
    var flatRDD = rdd.flatMap(
      List => {
        List
      }
    )
    flatRDD.collect().foreach(println)
    sc.stop()
  }
}
