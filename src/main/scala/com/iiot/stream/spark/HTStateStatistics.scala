package com.iiot.stream.spark

import java.io.IOException
import com.iiot.stream.base.DPUnion
import com.iiot.stream.base.RedisOperation
import org.apache.log4j.Logger
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis


/**
  * Created by liu on 2017-07-21.
  *
  */
class HTStateStatistics extends Serializable {
  val activeInterval: Int = 30

  @transient lazy val logger: Logger = {
    Logger.getLogger(classOf[HTStateStatistics])
  }

  //    1 统计所有datapoint数量
  def totalDP(jsonDStream: DStream[DPUnion]): Unit = {
    jsonDStream.map(x => (x.getTid, 1))
      .foreachRDD(rdd => {
        rdd.foreachPartition(record => {
          var redisHandle :Jedis= null
          var num =0
          try{
            redisHandle = RedisOperation.getResource()
          record.foreach(
            x => {
              num = num + x._2
            })
            redisHandle.incrBy("htstream:total:dp", num)
            logger.info("the number of dataPoints, redis key = stream:total:dp,incryBy =" + num)
            logger.info("-------------------------------incrBY to redis")
          }catch {
            case e: IOException => {
              logger.error("can not get redis resource!" + e.getMessage)
            }
          }
          finally {
              try{
                redisHandle.close()
              }
              catch {
                case e:IOException => {
                  logger.error("can not get redis resource!"+ e.getMessage)
                }
              }
          }

        })
      })
 }

  //    2 按租户统计所有的DataPoint数量 "stream:total:dp:tenantid:21"
  def totalTenanidDP(jsonDStream: DStream[DPUnion]): Unit = {
    jsonDStream.map(x => (x.getTid,1)).reduceByKey(_ + _)
      .foreachRDD(rdd => {
        rdd.foreachPartition(record => {
          var redisHandle :Jedis = null
          try {
            redisHandle = RedisOperation.getResource()
            record.foreach(
              x => {
                redisHandle.incrBy("htstream:total:dp:tenanid:" + x._1, x._2)
                logger.info("the number of dataPoints, redis key = stream:total:dp,incryBy =" + x._2)
                logger.info("-------------------------------incrBY to redis")
              })
          }catch {
              case e:IOException =>{
                logger.error("can not get redis resource!"+ e.getMessage)
              }
            }finally {
            try{
              redisHandle.close()
            } catch {
              case e:IOException => {
                logger.error("can not get redis resource!"+ e.getMessage)
              }
            }
          }
              })
        })
  }

  //    3 按租户与天数统计所有的DataPoint数量 "stream:total:dp:tenantid:103:date:2017-06-25"
  def totalTenantidDateDP(jsonDStream: DStream[DPUnion]): Unit = {
    jsonDStream.map(x => ((x.getTid, x.getTs), 1))
    .reduceByKey(_ + _)
      .foreachRDD(rdd => {
        rdd.foreachPartition(record => {
          var redisHandle :Jedis = null
          try {
            redisHandle = RedisOperation.getResource()
            record.foreach(
              x => {
                redisHandle.incrBy("htstream:total:dp:tenantid:" + x._1._1 + ":date:" + x._1._2, x._2)
                logger.info("the number of dataPoints based on tenantId and date, redis key=stream:total:dp:tenantid:" + x._1._1 + ":date:" + x._1._2 + ",incryBy =" + x._2)
              })
          }catch {
              case e: IOException => {
                logger.error("can not get redis resource!" + e.getMessage)
              }
            }
            finally {
              try {
                redisHandle.close()
              } catch {
                case e: IOException => {
                  logger.error("can not get redis resource!" + e.getMessage)
                }
              }
            }
          })
      })
  }


  //    4 按日期统计所有的DataPoint数量 total:dp:date  "stream:total:dp:date:2017-07-13"
  def totalDateDP(jsonDStream: DStream[DPUnion]): Unit = {
    jsonDStream.map(x => (x.getTs, 1))
      .reduceByKey(_ + _)
      .foreachRDD(rdd => {
        rdd.foreachPartition(record => {
          var redisHandle :Jedis = null
          try {
            redisHandle = RedisOperation.getResource()
            record.foreach(
              x => {
               redisHandle.incrBy("htstream:total:dp:date:" + x._1, x._2)
                logger.info("the number of dataPoints based on date, redis key=stream:total:dp:date:" + x._1 + ",incryBy=" + x._2)
              })
          }catch {
            case e: IOException => {
              logger.error("can not get redis resource!" + e.getMessage)
            }
          }
          finally {
            try {
              redisHandle.close()
            } catch {
              case e: IOException => {
                logger.error("can not get redis resource!" + e.getMessage)
              }
            }
          }
        })
      })
  }


  //    5 按组件数统计所有的DataPoint数量   "stream:total:dp:compid:55551" compid=dsn+ 测点
  def totalDPCompid(jsonDStream: DStream[DPUnion]): Unit = {
    jsonDStream.map(x => (x.getCompId, 1)).reduceByKey(_ + _)
      .foreachRDD(rdd => {
        rdd.foreachPartition(record => {
          var redisHandle :Jedis = null
          try {
            redisHandle = RedisOperation.getResource()
            record.foreach(
              x => {
                redisHandle.incrBy("htstream:total:dp:compid:" + x._1, x._2)
                logger.info("the number of dataPoints based on compid, redis key=stream:total:dp:compid:" + x._1 + ",incryBy =" + x._2)
              })
          }catch {
            case e: IOException => {
              logger.error("can not get redis resource!" + e.getMessage)
            }
          }
          finally {
            try {
              redisHandle.close()
            } catch {
              case e: IOException => {
                logger.error("can not get redis resource!" + e.getMessage)
              }
            }
          }
        })
      })
  }


  //6 所有的独立dn数 unique: dn
  def uniqueDN(jsonDStream: DStream[DPUnion]): Unit = {
    jsonDStream.map(x => (x.getDn, 1)).reduceByKey(_ + _)
      .foreachRDD(rdd => {
        rdd.foreachPartition(record => {
          var redisHandle :Jedis = null
          try {
            redisHandle = RedisOperation.getResource()
            record.foreach(
              x => {
                //redisHandle.sadd("htstream:unique:dn", x._2.toString)
                redisHandle.set("htstream:unique:dn", x._2.toString)
                logger.info("the number of unique dn, redis key = stream:unique:dn,sadd=" + x._2)
              })
          }catch {
            case e: IOException => {
              logger.error("can not get redis resource!" + e.getMessage)
            }
          }
          finally {
            try {
              redisHandle.close()
            } catch {
              case e: IOException => {
                logger.error("can not get redis resource!" + e.getMessage)
              }
            }
          }
        })
      })
  }



  //    7 所有独立的设备数 unique:thingid
  def uniqueThingid(jsonDStream: DStream[DPUnion]): Unit = {
    jsonDStream.map(x => (x.getThingId, 1)).reduceByKey(_ + _)
      .foreachRDD(rdd => {
        rdd.foreachPartition(record => {
          var redisHandle :Jedis = null
          try {
            redisHandle = RedisOperation.getResource()
            record.foreach(
              x => {
                redisHandle.set("htstream:unique:thingid", x._2.toString)
                logger.info("the number of unique thingId, redis key = stream:unique:thingid,sadd=" + x._2)
              })
          }catch {
            case e: IOException => {
              logger.error("can not get redis resource!" + e.getMessage)
            }
          }
          finally {
            try {
              redisHandle.close()
            } catch {
              case e: IOException => {
                logger.error("can not get redis resource!" + e.getMessage)
              }
            }
          }
        })
      })
  }


  //    8 按天统计去重设备数 unique:thingid:date  stream:unique:thingid:date:2017-06-26
  def uniqueThingidDate(jsonDStream: DStream[DPUnion]): Unit = {
    jsonDStream.map(x => ((x.getTs, String.valueOf(x.getThingId)), 1)
    ).reduceByKey(_ + _)
      .foreachRDD(rdd => {
        rdd.foreachPartition(record => {
          var redisHandle :Jedis = null
          try {
            redisHandle = RedisOperation.getResource()
            record.foreach(
              x => {
                redisHandle.set("htstream:unique:thingid:date:" + x._1._1, x._2.toString)
                logger.info("the number of unique thingId based on date, redis key = stream:unique:thingid:date:" + x._1._1 + ",sadd=" + x._2)
              })
          }catch {
            case e: IOException => {
              logger.error("can not get redis resource!" + e.getMessage)
            }
          }
          finally {
            try {
              redisHandle.close()
            } catch {
              case e: IOException => {
                logger.error("can not get redis resource!" + e.getMessage)
              }
            }
          }
        })
      })
  }


  //    9 按租户与天数统计所有去重的设备数 stream:unique:thingid:tenantid:date
  def uniqueThingidTenantidDate(jsonDStream: DStream[DPUnion]): Unit = {
    jsonDStream.map(x => ((x.getTid, x.getTs, String.valueOf(x.getThingId)), 1)
   ).reduceByKey(_ + _)
      .foreachRDD(rdd => {
        rdd.foreachPartition(record => {
          var redisHandle :Jedis = null
          try {
            redisHandle = RedisOperation.getResource()
            record.foreach(
              x => {
                redisHandle.set("htstream:unique:thingid:tenantid:" + x._1._1 + ":date:" + x._1._2, x._2.toString)
                logger.info("the number of unique thingId based on tenantId and date, redis key = stream:unique:thingid:tenantid:" + x._1._1 + ":date:" + x._1._2 + ",sadd=" + x._2)
              })
          }catch {
            case e: IOException => {
              logger.error("can not get redis resource!" + e.getMessage)
            }
          }
          finally {
            try {
              redisHandle.close()
            } catch {
              case e: IOException => {
                logger.error("can not get redis resource!" + e.getMessage)
              }
            }
          }
        })
      })
  }


  //    10 按天与租户ID统计测点数 stream:unique:compid:tenantid:60:date:2017-06-27
  def dateTidCompid(jsonDStream: DStream[DPUnion]): Unit = {
    jsonDStream.map(x =>  ((x.getCompId, x.getTid, x.getTs), 1)
    ).reduceByKey(_ + _)
      .foreachRDD(rdd => {
        rdd.foreachPartition(record => {
          var redisHandle :Jedis = null
          try {
            redisHandle = RedisOperation.getResource()
            record.foreach(
              x => {
                redisHandle.set("htstream:unique:compid:tenantid:" + x._1._1 + ":date:" + x._1._3, x._2.toString)
                logger.info("the number of unique compId based on tenantId and date, redis key = stream:unique:compid:tenantid:" + x._1._1 + ":date:" + x._1._3 + ",sadd=" + x._2)
              })
          }catch {
            case e: IOException => {
              logger.error("can not get redis resource!" + e.getMessage)
            }
          }
          finally {
            try {
              redisHandle.close()
            } catch {
              case e: IOException => {
                logger.error("can not get redis resource!" + e.getMessage)
              }
            }
          }
        })
      })
  }


  //  11 30秒内独立dn数 "unique:dn:active:" + activeInterval  stream:unique:dn:active:30  "1000"
  def uniqueDnActive(jsonDStream: DStream[DPUnion]): Unit = {
    jsonDStream.map(x => (x.getDn, 1)).reduceByKeyAndWindow(_ + _, Seconds(activeInterval)).count()
      .foreachRDD(rdd => {
        rdd.foreachPartition(record => {
          var redisHandle :Jedis = null
          try {
            redisHandle = RedisOperation.getResource()
          record.foreach(
            x => {
              redisHandle.set("htstream:unique:dn:active:30", x.toString)
              logger.info("the number of unique dn in " + activeInterval + " seconds, redis key = stream:unique:dn:active:" + activeInterval + ",set =" + x.toString)
            })
          }catch {
            case e: IOException => {
              logger.error("can not get redis resource!" + e.getMessage)
            }
          }
          finally {
            try {
              redisHandle.close()
            } catch {
              case e: IOException => {
                logger.error("can not get redis resource!" + e.getMessage)
              }
            }
          }
        })
      })
  }

  //  12 根据租户ID统计30秒内独立thingid数 "stream:unique:thingid:active:30:tenantid:105"
  def uniqueThingidTenantidActive(jsonDStream: DStream[DPUnion]): Unit = {
    jsonDStream.map(x => ((x.getTid, x.getThingId), 1))
      .reduceByKeyAndWindow(_ + _, Seconds(activeInterval)).map(x => (x._1._1, 1)).reduceByKey(_ + _)
      .foreachRDD(rdd => {
        rdd.foreachPartition(record => {
          var redisHandle :Jedis = null
          try {
            redisHandle = RedisOperation.getResource()
          record.foreach(
            x => {
              redisHandle.set("htstream:unique:thingid:active:" + activeInterval + ":tenantid:" + x._1, x._2.toString)
              logger.info("the number of unique thingId in " + activeInterval + " seconds based on tenantId, ,redis key = stream:unique:thingid:active:" + activeInterval + ":tenantid:" + x._1 + ",set =" + x._2.toString)
            })
         }catch {
            case e: IOException => {
              logger.error("can not get redis resource!" + e.getMessage)
            }
          }
          finally {
            try {
              redisHandle.close()
            } catch {
              case e: IOException => {
                logger.error("can not get redis resource!" + e.getMessage)
              }
            }
          }
          }
        )
      })
  }

  //  13 根据租户ID统计30秒内独立compid数 stream:unique:compid:active:30:tenantid:1
  def dateTidCompidActive(jsonDStream: DStream[DPUnion]): Unit = {
    jsonDStream.map(x => ((x.getTid, x.getCompId), 1))
      .reduceByKeyAndWindow(_ + _, Seconds(activeInterval)).map(x => (x._1._1, 1)).reduceByKey(_ + _)
      .foreachRDD(rdd => {
        rdd.foreachPartition(record => {
          var redisHandle :Jedis = null
          try {
            redisHandle = RedisOperation.getResource()
            record.foreach(
              x => {
                redisHandle.set("htstream:unique:compid:active:" + activeInterval + ":tenantid:" + x._1, x._2.toString)
                logger.info("the number of unique compid in " + activeInterval + " seconds based on tenantId，redis key = stream:unique:compid:active:" + activeInterval + ":tenantid:" + x._1 + ",set =" + x._2.toString)
              })
          }catch {
            case e: IOException => {
              logger.error("can not get redis resource!" + e.getMessage)
            }
          }
          finally {
            try {
              redisHandle.close()
            } catch {
              case e: IOException => {
                logger.error("can not get redis resource!" + e.getMessage)
              }
            }
          }
        }
        )
      })
  }


}
