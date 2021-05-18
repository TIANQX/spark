package com.zd.bigdata.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "F:\\BigDataProgramFile\\winutils")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)

    // TODO  算子 map
    var rdd = sparkContext.makeRDD(List(1, 2, 3, 4));

    // 转换函数
    def mapFunction(num: Int): Int = {
      num * 2
    }

//    val mapRdd: RDD[Int] = rdd.map(mapFunction)
//
//    val mapRdd: RDD[Int] = rdd.map((num: Int) => {
//      num * 2
//    })
//    val mapRdd: RDD[Int] = rdd.map((num: Int) => num * 2)
//    val mapRdd: RDD[Int] = rdd.map((num) => num * 2)
    val mapRdd: RDD[Int] = rdd.map(_ * 2)
    mapRdd.collect().foreach(println)
  }
}
