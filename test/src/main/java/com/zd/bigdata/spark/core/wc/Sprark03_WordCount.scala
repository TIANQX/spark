package com.zd.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Sprark03_WordCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "F:\\BigDataProgramFile\\winutils")
    //application
    //spark框架
    //TODO 1.建立和spark框架的连接
    //jdbc：connection
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    //TODO 2.执行业务操作

    //1。读取文件，获取一行一行的数据
    //hello world
    val lines: RDD[String] = sc.textFile("datas")
    //2.将一行一行的数据进行拆分，形成一个一个的单词（分词） 扁平化操作
    // "hello word"=>hello,world,hello,world
    val words: RDD[String] = lines.flatMap(_.split(" ")) //质检原则，匿名函数可以简化
    //3.
    val wordToOne: RDD[(String, Int)] = words.map(
      word => (word, 1)
    )
    /*spark框架提供了更多的功能，可以将分组和聚合使用一个方法实现*/
   /*reduceByKey:相同的key的数据，可以对value进行聚合*/

    //    wordToOne.reduceByKey((x,y)=>{x+y})
    //    wordToOne.reduceByKey((x,y)=>x+y)
    val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _) //匿名函数的自检原则
    //5.将转换结果采集到控制台打印出来
    val array = wordToCount.collect()
    array.foreach(println)

    //TODO 3.关闭连接
    sc.stop()
  }
}
