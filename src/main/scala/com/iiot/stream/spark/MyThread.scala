package com.iiot.stream.spark

import java.io.IOException
import java.util
import com.iiot.stream.base.RedisOperation
import com.htiiot.resources.model.ThingBullet
import com.htiiot.store.model.{DataPoint, Metric}
import com.iiot.stream.tools.KafkaWriter
import org.apache.log4j.Logger
import redis.clients.jedis.Jedis
import com.iiot.stream.base.{DPConsumerPlugin, DPUnion}


class MyThread(name:String,num:Int,arr:Array[ DataPoint],hashMap:util.HashMap[String,String]) extends Thread{
  @transient lazy val logger: Logger = Logger.getLogger(classOf[MyThread])

  var _name = name
  var _num = num
  var _hashMap =hashMap

override def run(){
    val start = System.currentTimeMillis()
    if (arr.length > 0) {
      var redisHandle :Jedis = null
      val plugin = DPConsumerPlugin.getPlugin()
      try {
        redisHandle = RedisOperation.getResource()
        for (data <- arr) {
          if (data != null) {
            var thingBullets: Array[ThingBullet] = null
            thingBullets = plugin.checkDataPointToThingBullet(data, redisHandle,_hashMap)
            if (null == thingBullets) {
            } else {
              KafkaWriter.getResource()
              KafkaWriter.writer(thingBullets)
              thingBullets = null
            }
          }
        }
      }catch {
        case e: IOException => {
          logger.error("IO Error" + e.getMessage)
        }
        case e:NumberFormatException =>{
          logger.error("NumberFormat Error"+ e.getMessage)
        }
        case e:Exception =>{
          logger.error("Unknown Error"+ e.getMessage)
        }
      }
      finally {
        try {
          redisHandle.close()
        } catch {
          case e: IOException => {
            logger.error("IO Error" + e.getMessage)
          }
        }
      }
      val end = System.currentTimeMillis()
      //println("in the thread the process time is:" + (end - start))
    }
  }
}
