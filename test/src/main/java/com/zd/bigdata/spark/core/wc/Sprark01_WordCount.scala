package com.zd.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Sprark01_WordCount {
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
    //3.将数据根据单词进行分组，便于统计
    //(hello,hello,hello),(world,world)
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word) //给的每个元素是单词，按照元素进行分组
    //4.对分组后的数据进行转换
    //(hello,hello,hello),(world,world)=> (hello,3),(world,2)
    val wordToCount: RDD[(String, Int)] = wordGroup.map {
      //模式匹配
      case (word, list) => {
        (word, list.size)
      }
    }
    //5.将转换结果采集到控制台打印出来
    val array = wordToCount.collect()
    array.foreach(println)

    //TODO 3.关闭连接
    sc.stop()
  }
}
