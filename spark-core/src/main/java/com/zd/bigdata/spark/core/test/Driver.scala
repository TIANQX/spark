package com.zd.bigdata.spark.core.test

import java.net.Socket

/**
 * @Author tqx
 * @CreateDate 2021/5/10
 * @Description TODO
 */
object Driver {
  def main(args: Array[String]): Unit = {
    //连接服务器
    val client = new Socket("localhost", 9999)
    val outputStream = client.getOutputStream
    outputStream.write(3)

    outputStream.flush()
    outputStream.close()
    client.close()
  }
}
