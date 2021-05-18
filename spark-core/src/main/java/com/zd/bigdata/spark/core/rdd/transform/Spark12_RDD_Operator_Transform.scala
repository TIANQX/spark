package com.zd.bigdata.spark.core.rdd.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "F:\\BigDataProgramFile\\winutils")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    //sortBy
    val rdd = sparkContext.makeRDD(List(5, 2, 1, 7, 6, 8), 2)
    val newRDD = rdd.sortBy(num => num)
    newRDD.saveAsTextFile("output")
    sparkContext.stop()

  }
}
