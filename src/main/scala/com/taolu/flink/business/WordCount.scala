package com.taolu.flink.business

import org.apache.flink.streaming.api.scala._

object WordCount {

  def main(args: Array[String]): Unit = {

    //编程入口
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //读取9000端口的以换行为结尾的数据
    val inputText = env.socketTextStream("localhost",9000,'\n')

    //将读到的数据进行空格切分
    val splitText = inputText.flatMap(x => x.split("\\s"))

    //转换为kv二元组
    val mapText = splitText.map(x => (x,1))

    //计数,以二元组的第一个值为key
    val sum = mapText.keyBy(0).sum(1)

    //单线程打印
    sum.print().setParallelism(1)

    env.execute("Word Count Test!")

  }

}
