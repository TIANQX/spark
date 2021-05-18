package com.zd.bigdata.spark.core.rdd.transform

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "F:\\BigDataProgramFile\\winutils")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sparkContext = new SparkContext(sparkConf)
    val rdd = sparkContext.textFile("datas/apache.log")
    val timeRDD = rdd.map(
      line => {
        val datas = line.split(" ")
        val time = datas(3)
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val date = simpleDateFormat.parse(time)
        val simpleDateFormat1 = new SimpleDateFormat("HH")
        val hour = simpleDateFormat1.format(date)
        (hour, 1)

      }

    ).groupBy(_._1)

    timeRDD.map({
      case (hour, iter) => {
        (hour, iter.size)
      }
    }).collect().foreach(println)

    sparkContext.stop()
  }

}
