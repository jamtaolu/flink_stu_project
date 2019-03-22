package com.taolu.flink.source

import java.util.Properties

object KafkaSource {

  //创建kafka配置
  def getProperties():Properties = {
    val pro = new Properties()
    pro.setProperty("bootstrap.servers","127.0.0.1:9092")
    pro.setProperty("group.id","lutaoConsumer")
    pro.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    pro.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    pro.setProperty("enable.auto.commit","false")
    pro.setProperty("auto.offset.reset","earliest")
    pro
  }

}
