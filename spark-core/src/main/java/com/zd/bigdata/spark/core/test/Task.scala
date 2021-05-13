package com.zd.bigdata.spark.core.test

/**
 * @Author tqx
 * @CreateDate 2021/5/10
 * @Description TODO
 */
class Task extends Serializable {

  var datas = List(1, 2, 3, 4)

  val logic = (num: Int) => {
    num * 2
  }

  val logic2: (Int) => Int = _ * 2

  //计算
  def compute(): List[Int] = {
    datas.map(logic)
  }

}
