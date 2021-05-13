package com.zd.bigdata.spark.core.test

import java.io.ObjectOutputStream
import java.net.Socket

/**
 * @Author tqx
 * @CreateDate 2021/5/10
 * @Description TODO
 */
object Driver2 {
  def main(args: Array[String]): Unit = {
    //连接服务器
    val client = new Socket("localhost", 9999)

    val outputStream = client.getOutputStream
    val objectOutputStream = new ObjectOutputStream(outputStream)
    val task = new Task()
    objectOutputStream.writeObject(task)

    objectOutputStream.flush()
    objectOutputStream.close()
    client.close()
    println("客户端数据发送完毕")
  }
}
