package com.zd.bigdata.spark.core.rdd.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark09_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "F:\\BigDataProgramFile\\winutils")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)
    //distince
    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4, 1, 2, 3, 4))
    val rdd1 = rdd.distinct()
    rdd1.collect().foreach(println)

    sparkContext.stop()
  }
}
