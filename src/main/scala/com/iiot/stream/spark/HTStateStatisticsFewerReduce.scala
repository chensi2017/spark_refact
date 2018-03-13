package com.iiot.stream.spark


import java.io.IOException
import com.iiot.stream.base._
import com.iiot.stream.tools.TimeTransform
import org.apache.log4j.Logger
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis
import scala.collection.mutable.HashMap

class HTStateStatisticsFewerReduceextends extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(classOf[HTStateStatisticsFewerReduceextends])

  val activeInterval: Int = 30

  val foreachPartitionFunc = (it: Iterator[DPUnion]) => {
    var redisHandle :Jedis = null
    try {
      redisHandle = RedisOperation.getResource()
    }catch {
      case e:IOException =>{
        logger.error("IO Error" + e.getMessage)
      }
    }
    //1,DP总数
    var numTotalDP = 0
    //2,按tid统计x总数
    var hashMapByTid= new HashMap[String,Int]
    //3,按date统计x总数
    var hashMapByDate= new HashMap[String,Int]
    //4,按date和tid统计x总数
    var hashMapByTidDate = new HashMap[Tuple2[String,String],Int]

    try {
      while (it.hasNext) {
        val x = it.next()
        val date = TimeTransform.timestampToDate(x.getTs)
        //1,统计所有DP总数
        numTotalDP = numTotalDP + 1

        //2，按租户统计DP
        val tid = hashMapByTid.getOrElse(x.getTid, 0)
        hashMapByTid.put(x.getTid, tid.toInt + 1)

        //3，按日期统计DP
        val dateNum = hashMapByDate.getOrElse(date, 0)
        hashMapByDate.put(date, dateNum.toInt + 1)

        //4，x num by tid and date
        val tidDateNum = hashMapByTidDate.getOrElse((x.getTid, date), 0)
        hashMapByTidDate.put((x.getTid, date), tidDateNum.toInt + 1)
      }
    }catch {
      case e:NumberFormatException =>{
        logger.error("NumberFormat Error" + e.getMessage)
      }
      case e:NullPointerException =>{
        logger.error("NullPoint Error" + e.getMessage)
      }
    }

    try {
      //1,total dp
      println("the total  DP num is:" + numTotalDP)
      redisHandle.incrBy("htstream:total:dp", numTotalDP)

      //2,total dp by tid
      hashMapByTid.keys.foreach(x => {
        val num = hashMapByTid.get(x) match {
          case Some(a) => a
          case None => 0
        }
        logger.info("==============================================")
        logger.info("==============================================")
        logger.info("==============================================")
        logger.info("htstream:total:dp:tenanid:" + x + ";num:" + num)
        redisHandle.incrBy("htstream:total:dp:tenanid:" + x, num)
      })

      //3,total dp by date
      hashMapByDate.keys.foreach(x => {
        val num = hashMapByDate.get(x) match {
          case Some(a) => a
          case None => 0
        }
        logger.info("==============================================")
        logger.info("==============================================")
        logger.info("==============================================")
        logger.info("htstream:total:dp:date:" + x + ";num:" + num)
        redisHandle.incrBy("htstream:total:dp:date:" + x, num)
      })

      //4,dp num by tid and date
      hashMapByTidDate.keys.foreach(x => {
        val num = hashMapByTidDate.get(x) match {
          case Some(a) => a
          case None => 0
        }
        redisHandle.incrBy("htstream:total:dp:tenantid:" + x._1 + ":date:" + x._2, num.toLong)
      })
    }catch {
      case e:IOException =>{
        logger.error("IO Error" + e.getMessage)
      }
      case e:NullPointerException =>{
        logger.error("NullPoint Error" + e.getMessage)
      }
      case e:Exception =>{
        logger.error("Unknown Error" + e.getMessage)
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
  }

  def DPStatistics(jsonDStream: DStream[DPUnion]): Unit = {
    jsonDStream.foreachRDD(rdd => {
      println("the state rdd partitions size is :"+ rdd.partitions.size)
      rdd.foreachPartition(foreachPartitionFunc)
  })
  }


  def distinctDnThingID(jsonDStream: DStream[DPUnion]): Unit = {
    //5, distinct dn
      jsonDStream.map(x => (x.getDnStr)).foreachRDD( rdd => {
        var redisHandle: Jedis = null
        redisHandle = RedisOperation.getResource()
        val distinctDnNum = rdd.distinct().count()
        println("the distinct dn num is:" + distinctDnNum)
        redisHandle.set("htstream:unique:dn", distinctDnNum.toString)
        redisHandle.close()
      })

    //6, distinct thingID
    jsonDStream.map(x => (x.getThingId)).foreachRDD( rdd => {
      var redisHandle: Jedis = null
      redisHandle = RedisOperation.getResource()
      val distinctThingIdNum = rdd.distinct().count()
      println("the distinct dn num is:" + distinctThingIdNum)
      redisHandle.set("htstream:unique:dn", distinctThingIdNum.toString)
      redisHandle.close()
    })
  }


}
