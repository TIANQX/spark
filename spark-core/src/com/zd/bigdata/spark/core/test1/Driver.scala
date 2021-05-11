package com.zd.bigdata.spark.core.test1

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

/**
 * @Author tqx
 * @CreateDate 2021/5/10
 * @Description TODO
 */
object Driver {
  def main(args: Array[String]): Unit = {
    val client1 = new Socket("localhost", 8888)
    val client2 = new Socket("localhost", 9999)


    val outputStream1: OutputStream = client1.getOutputStream
    val objectOutputStream1 = new ObjectOutputStream(outputStream1)

    val task = new Task()
    val subTask1 = new SubTask()
    subTask1.datas = task.datas.take(2)
    subTask1.logic = task.logic

    objectOutputStream1.writeObject(subTask1)
    objectOutputStream1.flush()
    objectOutputStream1.close()
    client1.close()


    val outputStream2: OutputStream = client2.getOutputStream
    val objectOutputStream2 = new ObjectOutputStream(outputStream2)

    val subTask2 = new SubTask()
    subTask1.logic = task.logic
    subTask1.datas = task.datas.takeRight(2)

    objectOutputStream2.writeObject(subTask1)
    objectOutputStream1.flush()
    objectOutputStream1.close()
    client1.close()

  }


}
