package com.taolu.flink.producer

import java.io.File
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.taolu.flink.pojo.UserBehavior
import com.taolu.flink.util.ReadUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducerPv {

  def main(args: Array[String]): Unit = {

    val topic = "test"

    val csvFileUrl = KafkaProducerPv.getClass.getClassLoader.getResource("UserBehavior.csv")
    println(csvFileUrl)
    val file = new File(csvFileUrl.toURI)
    val dataList = ReadUtil.readFileStr(file)

    //获取生产者配置
    val produerPro = getProducerPro

    //生产者实例
    val producer = new KafkaProducer[String,String](produerPro)

    //构建100条用户行为数据
    for (i <-1 to 100 ){
      val oneUserData = dataList.get(i)
      val splitData:Array[String] = oneUserData.split(",")
      val userBehavior = new UserBehavior
      userBehavior.setUserId(splitData(0).toLong)
      userBehavior.setItemId(splitData(1).toLong)
      userBehavior.setCategoryId(splitData(2).toInt)
      userBehavior.setBehavior(splitData(3))
      userBehavior.setTimestamp(splitData(4).toLong)
      val json = JSON.toJSON(userBehavior).toString
      producer.send(new ProducerRecord(topic,i.toString,json))
      println(s"insert into test :${i} , ${json}")
    }


  }

  //构建生产者pro
  def getProducerPro:Properties = {
    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", "127.0.0.1:9092")
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//    kafkaProps.put("retries", 3)
    kafkaProps.put("acks", "all")
    kafkaProps.put("client.id", "tao.lu")
    kafkaProps
  }

}
