package com.iiot.stream.tools

import java.io.IOException

import com.alibaba.fastjson.{JSON, JSONObject}
import com.htiiot.store.model.{DataPoint, Metric}
import com.iiot.stream.base._
import org.apache.http.HttpEntity
import org.apache.http.client.{ClientProtocolException, HttpResponseException}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpPut}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils
import org.apache.log4j.Logger
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ArrayBuffer

/**
  * Created by liu on 2017-07-21.
  */
object HTInputDStreamFormat {
  @transient lazy val logger: Logger = Logger.getLogger(HTInputDStreamFormat.getClass)
  val zkClent = new ZookeeperClient
  val zk = zkClent.getConfingFromZk("172.26.223.113:2181", 30000)
  var configs = zkClent.getAll(zk, "/conf_htiiot/spark_streamming")
  val urlBase: String = configs.getProperty("urlBase")

  def inputDStreamFormat(stream: DStream[(String, String)]): DStream[DPUnion] = {
    val resultJson = stream.map(x => JSON.parseObject(x._2,classOf[DPList]))
      .map(x => dataTransform(x))
      .flatMap(x => x)
    resultJson
  }

  def inputDStreamFormatWithDN(stream: DStream[(String, String)]): DStream[DPUnion] = {
    val resultJson = stream.map(x => JSON.parseObject(x._2,classOf[DPListWithDN]))
      .map(x => dataTransformWithDN(x))
      .flatMap(x => x)
    resultJson.cache()
    resultJson
  }


  def inputDStreamFormat4Monitor(stream: DStream[(String, String)]): DStream[DataPoint] = {
    val resultJson = stream.map(x => JSON.parseObject(x._2, classOf[DPList]))
      .map(x => dataTransform4Monitor(x))
      .flatMap(x => x.toArray.toTraversable)
    resultJson.cache()
    resultJson
  }

  def dataTransform4Monitor(x: DPList): ArrayBuffer[DataPoint] = {
    logger.info("the raw string is " + x.toString())
    var ts: Long = 0L
    val tid = x.getTid
    val dsn = x.getDsn
    val data = x.getData
    var dn: String = "0"
    var compId = "0"
    var thingId = "0"
    var result = ArrayBuffer[DataPoint]()
    var redisHandle: Jedis = null
    try {
      redisHandle = RedisOperation.getResource()
      for (metric <- data) {
        if (metric.getTs.toString.isEmpty) {
          ts = metric.getTs
        } else {
          ts = metric.getTs
        }
        val metricName = metric.getK
        var value = metric.getV
        var valueDou = 0.0
        try {
          valueDou = value.toDouble
        } catch {
          case ex: NumberFormatException => {
            logger.error("vlaue format error")
            valueDou = 0.0
          }
          case ex: Exception => {
            logger.error("unknown error is value format")
          }
        }

        val dnFromCache = "AAAAAQAAABAAAAAAAANqzg=="
        //val dnFromCache = redisHandle.get("ht:dn:tid:" + tid + ":dsn:" + dsn + ":metric:" + metricName)
        if (dnFromCache != null) {
          dn = dnFromCache
          logger.info("get dn from redis which has been cached，dn = " + dn)
        } else {
          dn = getSingleDnFromUrl(tid, dsn, metricName)
          val key = "ht:dn:tid:" + tid + ":dsn:" + dsn + ":metric:" + metricName
          redisHandle.set(key, dn)
          logger.info("set key =" + key + ",value=" + dn + ",to redis which not has been cached")
        }

        if ((dn != "0") && (dn != null)) {
          compId = DeviceNumber.fromBase64String(dn).getMetricIdLong.toString
          thingId = DeviceNumber.fromBase64String(dn).getDeviceIdInt.toString
          logger.info("dn = " + dn + ",and compid = " + compId + ",thingid = " + thingId)
          /* if (value.nonEmpty && (! "0".equals(dn))) {
          result += {
            if ("".equals(result)) "" else "-"
          } +
            "{\"ts\":" + ts +
            ",\"dn\":"+ "\"" + dn + "\"" +
            ",\"value\":" + value +
            ",\"metric\":\"" + metric + "\"}"
        }
      }*/
          val dp = new DataPoint()
          val deviceNumber = com.htiiot.resources.utils.DeviceNumber.fromBase64String(dn)
          dp.setDeviceNumber(deviceNumber)
          dp.setTs(ts)
          dp.setMetric(new Metric(metricName, valueDou))
          result += dp
        }
      }
    }
    catch {
      case e: IOException => {
        logger.error("can not get redis resource!" + e.getMessage)
      }
    } finally {
      try {
        redisHandle.close()
      } catch {
        case e: IOException => {
          logger.error("can not get redis resource!" + e.getMessage)
        }
      }
    }
    logger.info("the result is :" + result)
    result
  }

  def dataTransformWithDN(x: DPListWithDN): ArrayBuffer[DPUnion]={
    logger.info("the raw string is " + x.toString)
    var result = ArrayBuffer[DPUnion]()
    try {
      var ts: Long = x.getTs
      val tid = x.getTid
      val dsn = x.getDid
      var data = x.getData
      var dn: String = "0"
      var compId = "0"
      var thingId = "0"
      for (metric <- data) {
        if (metric.getTs.toString.nonEmpty) {
          ts = metric.getTs
        }
        val metricName = metric.getK
        val metricValue = metric.getV
        dn = metric.getDN

        if ((dn != "0") && (dn != null)) {
          //compId = DeviceNumber.fromBase64String(dn).getMetricIdLong.toString
          //thingId = DeviceNumber.fromBase64String(dn).getDeviceIdInt.toString
          val dp = new DPUnion()
          //val deviceNumber = com.htiiot.resources.utils.DeviceNumber.fromBase64String(dn)
          val deviceNumber = com.htiiot.resources.utils.DeviceNumber.fromHexString(dn)
          deviceNumber.getComponentId
          deviceNumber.getThingId
          dp.setDn(deviceNumber)
          dp.setTs(ts)
          dp.setKey(metricName)
          dp.setCompId(deviceNumber.getComponentId.toString)
          dp.setTid(tid)
          dp.setThingId(deviceNumber.getThingId.toString)
          dp.setValue(metricValue)
          result += dp
        }
      }
    }catch {
      case e:NullPointerException =>{
        logger.error("NullPointer Error"+e.getMessage)
      }
      case e:NumberFormatException =>{
        logger.error("NumberFormat Error"+ e.getMessage)
      }
      case e:Exception =>{
        logger.error("Unknown Error"+ e.getMessage)
      }
    }
    logger.info("the result string is :" + result)
    logger.info("the result string  num is :" + result.size)
    result
  }

  def dataTransform(x: DPList): ArrayBuffer[DPUnion] = {
    logger.info("the raw string is " + x.toString)
    var ts: Long = x.getTs
    val tid = x.getTid
    val dsn = x.getDsn
    var data = x.getData
    var result = ArrayBuffer[DPUnion]()
    var dn: String = "0"
    var compId = "0"
    var thingId = "0"
    for (metric <- data){
        if (metric.getTs.toString.nonEmpty) {
          ts = metric.getTs
        }
        val metricName = metric.getK
        val metricValue = metric.getV
        val dnFromCache = "AAAAAQAAABAAAAAAAANqzg=="
        dn = dnFromCache
        if ((dn != "0") && (dn != null)) {
          //compId = DeviceNumber.fromBase64String(dn).getMetricIdLong.toString
          //thingId = DeviceNumber.fromBase64String(dn).getDeviceIdInt.toString
          val dp = new DPUnion()
          val deviceNumber = com.htiiot.resources.utils.DeviceNumber.fromBase64String(dn)
          deviceNumber.getComponentId
          deviceNumber.getThingId
          dp.setDn(deviceNumber)
          dp.setTs(ts)
          dp.setKey(metricName)
          dp.setCompId(deviceNumber.getComponentId.toString)
          dp.setTid(tid)
          dp.setThingId(deviceNumber.getThingId.toString)
          dp.setValue(metricValue)
          result += dp
        }
      }
    logger.info("the result string is :" + result)
    logger.info("the result string  num is :" + result.size)
    result
  }



  def getSingleDnFromUrl(tid: String, dsn: String, metric: String): String = {
    var httpData: String = null
    var dn: String = "0"
    var response: CloseableHttpResponse = null
    var httpClient: CloseableHttpClient = null
    try {
      //val httpGet: HttpGet = new HttpGet("http://47.92.85.56:7020/api/component/tid/" + tid + "/dn/" + dsn + "/metric/" + metric)
      //http://106.74.146.202:8080/api-0.0.1/api/component/tid/1/dn/SLHP-ChangZhou-001/metric/ProjectPath
      val httpGet: HttpGet = new HttpGet(urlBase + tid + "/dn/" + dsn + "/metric/" + metric)

      logger.info(" receiving single dn, url = " + httpGet.getURI)
      httpClient = HttpClients.createDefault()
      try {
        response = httpClient.execute(httpGet)
        val entity: HttpEntity = response.getEntity
        if (entity != null) {
          httpData = EntityUtils.toString(entity)
          logger.info(" dn received , the result is ：" + httpData)
        }
      } catch {
        case e: HttpResponseException => e.printStackTrace()
        case e: Exception => e.printStackTrace()
      } finally {
        try {
          response.close()
        }
        catch {
          case e: IOException => e.printStackTrace()
        }
      }
    } catch {
      case e: ClientProtocolException => e.printStackTrace()
      case e: IOException => e.printStackTrace()
    } finally {
      try {
        httpClient.close()
      } catch {
        case e: IOException => e.printStackTrace()
      }
    }
    if (!httpData.isEmpty) {
      val httpDataObj: JSONObject = JSON.parseObject(httpData).getJSONObject("data")
      if (null != httpDataObj) {
        dn = httpDataObj.getString("dn")
        logger.info("get dn from url，dn = " + dn)
      }
    }
    dn
  }
}