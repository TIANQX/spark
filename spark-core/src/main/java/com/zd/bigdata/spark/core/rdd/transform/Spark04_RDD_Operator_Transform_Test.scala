package com.zd.bigdata.spark.core.rdd.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "F:\\BigDataProgramFile\\winutils")
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(List(1, 2), 3, List(4, 5)))

    val fladRDD = rdd.flatMap(
      data => {
        data match {
          case list: List[_] => {
            list
          }
          case dat => {
            List(dat)
          }
        }
      }
    )
    fladRDD.collect().foreach(println)
    sc.stop()
  }
}
