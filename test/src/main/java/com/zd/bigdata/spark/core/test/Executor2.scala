package com.zd.bigdata.spark.core.test

import java.io.ObjectInputStream
import java.net.ServerSocket

/**
 * @Author tqx
 * @CreateDate 2021/5/10
 * @Description TODO
 */
object Executor2 {
  def main(args: Array[String]): Unit = {
    //启动服务器，接收数据
    val serverSocket = new ServerSocket(9999)
    println("服务器启动，等待接收数据")
    //等待客户端连接
    val client = serverSocket.accept()
    val inputStream = client.getInputStream
    val objectInputStream = new ObjectInputStream(inputStream)
    val task: Task = objectInputStream.readObject().asInstanceOf[Task]
    val ints: List[Int] = task.compute()
    println("计算节点计算的结果为：" + ints);
    objectInputStream.close()
    client.close()
    serverSocket.close()
  }
}
