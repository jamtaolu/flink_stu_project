package com.taolu.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object UserBehaviorAnalyes {

  def main(args: Array[String]): Unit = {

    //编程入口
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //获取kafka配置
    val pro = getProperties

    //创建kafkaConsumer
    val kafkaConsumer = new FlinkKafkaConsumer010("test",new SimpleStringSchema(),pro)

    //配置消费者从最新的地方开始消费
    kafkaConsumer.setStartFromLatest()

    //创建kafkaSource
    val kafkaSource = env.addSource(kafkaConsumer)


  }



  //创建kafka配置
  def getProperties():Properties = {
    val pro = new Properties()
    pro.setProperty("bootstrap.servers","127.0.0.1:9092")
    pro.setProperty("group.id","tao.lu")
    pro.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    pro.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    pro.setProperty("enable.auto.commit","false")
    pro.setProperty("auto.offset.reset","earliest")
    pro
  }

}
