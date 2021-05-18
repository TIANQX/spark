package com.zd.bigdata.spark.core.rdd.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "F:\\BigDataProgramFile\\winutils")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val newRDD = rdd.repartition(3)
    newRDD.saveAsTextFile("output")

    sparkContext.stop()

  }
}
