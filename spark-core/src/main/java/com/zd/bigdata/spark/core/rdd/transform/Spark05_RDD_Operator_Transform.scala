package com.zd.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "F:\\BigDataProgramFile\\winutils")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)
    // 【1，2】，【3，4】
    // 【2】，【4】
    // 【6】
    // TODO  算子 glom
    var rdd = sparkContext.makeRDD(List(1, 2, 3, 4), 2)
    val glomRDD: RDD[Array[Int]] = rdd.glom()
    val maxRDD: RDD[Int] = glomRDD.map(
      array => {
        array.max
      }
    )
    println(maxRDD.collect().sum)
    sparkContext.stop()
  }
}
