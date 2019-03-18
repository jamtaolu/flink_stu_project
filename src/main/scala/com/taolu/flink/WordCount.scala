package com.taolu.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object WordCount {

  def main(args: Array[String]): Unit = {

    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("localhost",9000,'\n')

    text.print().setParallelism(1)

    env.execute("TEST!")

  }

}
