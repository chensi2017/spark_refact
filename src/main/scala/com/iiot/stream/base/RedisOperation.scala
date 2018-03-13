package com.iiot.stream.base

import java.{lang, util}

import com.iiot.stream.tools.ZookeeperClient
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.log4j.Logger
import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPool}
/**
  * Created by T数据部 on 2017/10/27.
  */
object RedisOperation {
  @transient lazy  val logger: Logger = Logger.getLogger(RedisOperation.getClass)

  var clusterNodeList: util.HashSet[HostAndPort] = _
  //val configs = new MemConfig("spark_redis.properties")
  val zkClent = new ZookeeperClient
  val zk = zkClent.getConfingFromZk("172.26.223.113:2181", 30000)
  var configs = zkClent.getAll(zk, "/conf_htiiot/redis")

  val hStr: String = configs.getProperty("address")
  val hList: Array[String] = hStr.split(",")
  for (host <- hList) {
    clusterNodeList = new java.util.HashSet[HostAndPort]()
    clusterNodeList.add(new HostAndPort(host, configs.getProperty("port").toInt))
  }

  //create a pool or cluster

  var redisCluster: Any = null
  var redisPattern: String = if("".equals(configs.getProperty("mode"))) "redispool" else configs.getProperty("mode")

  init()
  def init() {
    var poolConfig: GenericObjectPoolConfig = new GenericObjectPoolConfig
    poolConfig.setMinIdle(100)
    poolConfig.setMaxTotal(1000)
    poolConfig.setMaxWaitMillis(3000L)
    poolConfig.setMinEvictableIdleTimeMillis(1)
    poolConfig.setTestOnReturn(true)
    poolConfig.setTestWhileIdle(true)
    poolConfig.setMinEvictableIdleTimeMillis(60000)
    poolConfig.setTimeBetweenEvictionRunsMillis(30000)
    poolConfig.setNumTestsPerEvictionRun(-1)
    if ("cluster".equals(redisPattern)) {
      redisCluster = new JedisCluster(clusterNodeList,
        configs.getProperty("connectionTimeout").toInt,
        configs.getProperty("soTimeout").toInt,
        configs.getProperty("maxAttempts").toInt,
        //configs.get("redis.password"),
        poolConfig)
    } else if ("redispool".equals(redisPattern)) {
      redisCluster = new JedisPool(poolConfig,
        configs.getProperty("address"),
        configs.getProperty("port").toInt,
        configs.getProperty("connectionTimeout").toInt,
        configs.getProperty("auth"))
    }
    else {
      throw new Exception("you should check the ‘redis.running.pattern’to decide the redis' pattern.and " +
        "redispool and cluster is the option")
    }
  }


  def clusterIncrBy(key: String, addNum: Long): Long = {
    if(redisCluster == null){
      init()
    }
      val redisHandle = redisCluster.asInstanceOf[JedisCluster]
      val result = redisHandle.incrBy(key,addNum)
      result
  }

  def clusterSadd(key: String, vals: String): Long = {
    if(redisCluster == null){
      init()
    }
      val redisHandle = redisCluster.asInstanceOf[JedisCluster]
      val result = redisHandle.sadd(key,vals)
      result
  }

  def clusterSet(key: String, vals: String): String = {
    if(redisCluster == null){
      init()
    }
      val redisHandle = redisCluster.asInstanceOf[JedisCluster]
      val result = redisHandle.set(key,vals)
      result
  }

  def clusterGet(key: String): String = {
    if(redisCluster == null){
      init()
    }
      val redisHandle = redisCluster.asInstanceOf[JedisCluster]
      val result = redisHandle.get(key)
      result
  }

  def clusterDel(key: String): lang.Long = {
    if(redisCluster == null){
      init()
    }
      val redisHandle = redisCluster.asInstanceOf[JedisCluster]
      val result = redisHandle.del(key)
      result
  }


  def getResource()= {
    if(redisCluster == null){
      init()
    }
    val redisHandle = redisCluster.asInstanceOf[JedisPool].getResource
    redisHandle
  }
}
