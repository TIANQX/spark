package com.zd.bigdata.spark.core.test1

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

/**
 * @Author tqx
 * @CreateDate 2021/5/10
 * @Description TODO
 */
object Executor2 {
  def main(args: Array[String]): Unit = {
    val serverSocket = new ServerSocket(9999)
    println("服务器已启动，等待接收数据")
    val client: Socket = serverSocket.accept()
    val inputStream: InputStream = client.getInputStream
    val objectInputStream = new ObjectInputStream(inputStream)
    val subTask: SubTask = objectInputStream.readObject().asInstanceOf[SubTask]
    val ints: List[Int] = subTask.compute()

    println("计算节点【9999】的结算结果为：" + ints)

    objectInputStream.close()
    client.close()
    serverSocket.close()
  }


}
