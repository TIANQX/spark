package com.zd.bigdata.spark.core.rdd.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "F:\\BigDataProgramFile\\winutils")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List("Hello", "Spark", "Scala", "Hadoop"))
    val groupRDD = rdd.groupBy(_.charAt(0))
    groupRDD.collect().foreach(println)
    sparkContext.stop()
  }
}
