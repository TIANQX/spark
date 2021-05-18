package com.zd.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "F:\\BigDataProgramFile\\winutils")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    // TODO  算子 glom
    // 分区内取最大值，分区间最大值求和
    var rdd = sparkContext.makeRDD(List(1, 2, 3, 4), 2)
    // List => Int
    // Int => Array
    val glomRDD: RDD[Array[Int]] = rdd.glom()
    glomRDD.collect().foreach(data => println(data.mkString(",")))
    sparkContext.stop()
  }


}
