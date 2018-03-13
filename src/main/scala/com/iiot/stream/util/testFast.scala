package com.iiot.stream.util

import com.alibaba.fastjson
import com.alibaba.fastjson.JSON

import scala.collection.immutable.HashMap

class testFast {
  def str  =
  """
    {"tid":154,"dsn":"","ts":1504774577346,
 "data":[{"k":"整型3","v":0,"ts":1504803314145},
 {"k":"泵出液流","v":0.0,"ts":1504803314145},
 {"k":"$日.Value","v":7.0,"ts":1504803314145},
 |{"k":"整型3","v":0,"ts":1504803314145},
 | {"k":"泵出液流","v":0.0,"ts":1504803314145},
 | {"k":"$日.Value","v":7.0,"ts":1504803314145},
 | {"k":"整型3","v":0,"ts":1504803314145},
 | {"k":"泵出液流","v":0.0,"ts":1504803314145},
 | {"k":"$日.Value","v":7.0,"ts":1504803314145}]}
  """.stripMargin

  def testFastJson: Unit = {
    var map: HashMap.type = HashMap
    for (i <- 0 to 10000000) {
      val jsonParser =JSON.parseObject(str)
      val jSONArray = jsonParser.getJSONArray("data")
      val tid = jsonParser.get("tid")
      val dsn = jsonParser.get("dsn")
      val ts  = jsonParser.get("ts")
      for (i <- 0 until  jSONArray.size()){
        jSONArray.getJSONObject(i).get("k")
      }
    }
  }

}

object testFast{
  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    val tj = new testFast
    tj.testFastJson
    val end = System.currentTimeMillis()
    println("the time is:" + (end - start))
  }

}
