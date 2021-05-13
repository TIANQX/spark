package com.zd.bigdata.spark.core.test

import java.net.ServerSocket

/**
 * @Author tqx
 * @CreateDate 2021/5/10
 * @Description TODO
 */
object Executor {
  def main(args: Array[String]): Unit = {
    //启动服务器，接收数据
    val serverSocket = new ServerSocket(9999)
    println("服务器启动，等待接收数据")
    //等待客户端连接
    val client = serverSocket.accept()
    val inputStream = client.getInputStream
    val i = inputStream.read()
    println("接收到客户端发送的数据是：" + i);
    inputStream.close()
    client.close()
    serverSocket.close()
  }
}
