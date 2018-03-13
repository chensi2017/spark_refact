package com.iiot.stream.tools

import java.util.Properties

import com.fasterxml.jackson.databind.ObjectMapper
import com.htiiot.common.config.imp.ConfigClient
import com.htiiot.resources.model.ThingBullet
import kafka.common.KafkaException
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

/**
  * Created by yuxiao on 2017/9/15.
  * it is a single pattern.all the kafka operator use the only object.
  */

object KafkaWriter {
  //kf1.inter.htiiot.com
  val BROKER_LIST =ConfigClient.instance().get("spark_streamming","kafka.broker.list")
  val TOPIC = ConfigClient.instance().get("spark_streamming","monitor.kafka.topic")
  //should be a output topic
  var producer: KafkaProducer[String,String] = _


  def kafkaInit(): Unit = {
    val props = new Properties()
    props.put("metadata.broker.list", BROKER_LIST)
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    producer = new KafkaProducer[String, String](props)
  }

  def getResource(): Unit ={
    if(producer == null) kafkaInit()
    producer
  }

  def writer(thingBullets: Array[ThingBullet]): Unit = {
    if(thingBullets == null){
      //To Do something
    }else {
      var num =0
      for (thingBullet <- thingBullets) {
        //should follow the rules of kafka's event.
        val mapper = new ObjectMapper
        try {
          val jsonString: String = mapper.writeValueAsString(thingBullet)
          producer.send(new ProducerRecord(TOPIC, "bullet", jsonString))
          println("the bullet string is : " + jsonString)
        }catch {
          case e:KafkaException =>{
            println("event write to kafka is fault")
          }
        }
        println("send to kafka bullet's num is:" + num)
        num +=1
      }
    }
  }

  def destory() = {
    producer.close()
  }
}
