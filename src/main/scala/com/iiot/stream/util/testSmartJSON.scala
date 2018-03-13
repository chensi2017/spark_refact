package com.iiot.stream.util

import net.minidev.json.JSONObject
import net.minidev.json.JSONUtil
import net.minidev.json.JSONValue
import net.minidev.json.JSONArray
import net.minidev.json.JSONStyle
import net.minidev.json.parser.ParseException
import net.minidev.json.parser.JSONParser
import scala.collection.immutable.HashMap



class testSmart {

def str  =
  """
    {"tid":154,"dsn":"","ts":1504774577346,
 "data":[{"k":"整型3","v":0,"ts":1504803314145},
 | {"k":"泵出液流","v":0.0,"ts":1504803314145},
 | {"k":"$日.Value","v":7.0,"ts":1504803314145},
 | {"k":"整型3","v":0,"ts":1504803314145},
 | {"k":"泵出液流","v":0.0,"ts":1504803314145},
 | {"k":"$日.Value","v":7.0,"ts":1504803314145},
 | {"k":"整型3","v":0,"ts":1504803314145},
 | {"k":"泵出液流","v":0.0,"ts":1504803314145},
 | {"k":"$日.Value","v":7.0,"ts":1504803314145}]}
  """.stripMargin

def testSmartJson: Unit = {
  var map: HashMap.type = HashMap
  for (i <- 0 to 10000) {
    val jsonParser = new JSONParser()
    val jsonObj: JSONObject = jsonParser.parse(str).asInstanceOf[JSONObject]
    val tid = jsonObj.get("tid")
    val dsn = jsonObj.get("dsn")
    val ts  = jsonObj.get("ts")
    val jSONArray = jsonObj.get("data").asInstanceOf[JSONArray]
    val it = jSONArray.iterator()
    while (it.hasNext) {
      val itStr = it.next()
    }
  }
}

}

object testSmart{
  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    val tj = new testSmart
    tj.testSmartJson
    val end = System.currentTimeMillis()
    println("the time is:" + (end - start))
  }

}