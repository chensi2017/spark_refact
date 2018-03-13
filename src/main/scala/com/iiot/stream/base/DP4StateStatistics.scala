package com.iiot.stream.base

import com.htiiot.resources.utils.DeviceNumber
import com.iiot.stream.tools.TimeTransform

/**
  * Created by yuxiao on 2017-11-16.
  */

class DP4StateStatistics extends Serializable {
  var _dn:String = ""
  var _ts: Long = 0L
  var _tid: String = ""
  var _compId: String = ""
  var _thingId: String = ""
  var _metric: String = ""


  def getDn: String = _dn
  def setDn(value: String): Unit = {
    _dn = value
  }


  def getTs: String = TimeTransform.timestampToDate(_ts)
  def setTs(value: Long): Unit = {
    _ts = value
  }


  def getTid: String = _tid
  def setTid(value: String): Unit = {
    _tid = value
  }


  def getCompId: String = _compId
  def setCompId(value: String): Unit = {
    _compId = value
  }


  def getThingId: String = _thingId
  def setThingId(value: String): Unit = {
    _thingId = value
  }


  def getMetric: String = _metric
  def setMetric(value: String): Unit = {
    _metric = value
  }

}


