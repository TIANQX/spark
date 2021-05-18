package com.zd.bigdata.spark.core.rdd.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "F:\\BigDataProgramFile\\winutils")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(("1", 1), ("11", 2), ("2", 3)), 2)

    val newRDD = rdd.sortBy(t => t._1.toInt, false)
    newRDD.collect().foreach(println)
    sparkContext.stop()

  }
}
