package com.iiot.stream.tools

import java.io.IOException

import com.alibaba.fastjson.{JSON, JSONArray}
//import com.fasterxml.jackson.databind.ObjectMapper
//import com.fasterxml.jackson.core
import org.apache.http.HttpEntity
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.client.{ClientProtocolException, HttpResponseException}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils
import org.apache.log4j.Logger
import com.iiot.stream.base.RedisOperation
/**
  * Created by liu on 2017-07-27.
  */
class StoreDnToRedis {
  @transient lazy val logger: Logger = Logger.getLogger(StoreDnToRedis.getClass)
  def getAllDnFromUrl: String = {
    var httpData: String = ""
    var httpClient: CloseableHttpClient = null
    var response: CloseableHttpResponse = null
    try {
      //val httpGet: HttpGet = new HttpGet("http://106.74.146.202:8080/api-0.0.1/component/all");
      val httpGet: HttpGet = new HttpGet("http://106.74.146.202:8080/api/component/all");
      //http://106.74.146.204:8080/api/component/tid/1/dn/SLHP-ChangZhou-001/metric/Row
      //val httpGet = new HttpGet("http://47.92.92.70:8080/api-0.0.1/api/component/all")
      logger.info(" receiving all dn from url: " + httpGet.getURI)
      httpClient = HttpClients.createDefault()
      try {
        response = httpClient.execute(httpGet)
        val entity: HttpEntity = response.getEntity
        if (null != entity) {
          httpData = EntityUtils.toString(entity)
          logger.info(" dn received , the result is ：" + httpData.substring(1, 506) + "... ...")
        }
      } catch {
        case e: HttpResponseException => e.printStackTrace()
      } finally {
        response.close()
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
    logger.info("receiving all dn from url is completed...")
    httpData
  }

  def setDnToRedis(httpData: String): Unit = {
    /*  val mapper: ObjectMapper = new ObjectMapper()

    var node: ObjectNode = mapper.createObjectNode()
    val httpDataArray = mapper.createArrayNode
    httpDataArray.add(node.arrayNode())*/

    //try to changing the style of json string transferring.
    //but it is not worked.cannot use "mapper.readValue" method.
    val httpDataArray: JSONArray = JSON.parseObject(httpData).getJSONArray("data")
    logger.info("dn num is " + httpDataArray.size())
    var tid: Long = 0L
    var dsn, metric, dn: String = ""
    val redisPool = RedisOperation.getResource()
    var num = 0
    for (i <- 0 until httpDataArray.size()) {
      tid = httpDataArray.getJSONObject(i).getLong("tenantId")
      dsn = httpDataArray.getJSONObject(i).getString("dsn")
      metric = httpDataArray.getJSONObject(i).getString("metricName")
      dn = httpDataArray.getJSONObject(i).getString("dn")

      redisPool.set("ht:dn:tid:" + tid + ":dsn:" + dsn + ":metric:" + metric, dn)
      num += 1
      logger.info("set dn:" + dn + "------into redis" + "the num is " + num)
    }
    redisPool.close()
    logger.info("all dn store to redis is completed...")
  }
}

object StoreDnToRedis {
  def main(args: Array[String]): Unit = {
    val storeDnToRedis = new StoreDnToRedis
    val httpData: String = storeDnToRedis.getAllDnFromUrl
    storeDnToRedis.setDnToRedis(httpData)
  }

}

