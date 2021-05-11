package com.zd.bigdata.spark.core.test1

/**
 * @Author tqx
 * @CreateDate 2021/5/10
 * @Description TODO
 */
class SubTask extends Serializable {
  var datas: List[Int] = _
  var logic: (Int) => Int = _

  def compute(): List[Int] = {
    datas.map(logic)
  }
}
