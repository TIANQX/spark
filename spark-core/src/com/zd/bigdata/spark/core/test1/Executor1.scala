package com.zd.bigdata.spark.core.test1

import java.io.ObjectInputStream
import java.net.{ServerSocket, Socket}

/**
 * @Author tqx
 * @CreateDate 2021/5/10
 * @Description TODO
 */
object Executor1 {
  def main(args: Array[String]): Unit = {
    val serverSocket = new ServerSocket(8888)
    println("服务器已启动，等待接收数据")

    val socket: Socket = serverSocket.accept()
    val inputStream = socket.getInputStream
    val objectInputStream = new ObjectInputStream(inputStream)
    val subTask: SubTask = objectInputStream.readObject().asInstanceOf[SubTask]
    val ints: List[Int] = subTask.compute()

    println("计算节点【8888】计算的结果为："+ints)
    objectInputStream.close()
    socket.close()
    serverSocket.close()



  }


}
