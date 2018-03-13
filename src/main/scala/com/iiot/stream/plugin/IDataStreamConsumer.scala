package com.iiot.stream.plugin

import com.htiiot.resources.model.ThingBullet
import com.htiiot.store.model.DataPoint


/**
  * Created by yuxiao on 2017/7/5
  * it is the interface impl.ed by com.htiiot.alarm.DataStreamConsumer with thirdparty.
  * so the package name is "com.htiiot.stream.plugin".
  */
trait IDataStreamConsumer extends Serializable{
  def init(): Unit

  def destroy(): Unit

  def getVersion(): Long

  /**
    * 处理DataStream中的每一条记录。判断是否可以出发相关事件
    * @param componentId 传感器Id
    * @param metricName  传感器 具体参数名称
    * @param metricValue 传感器 具体参数值
    * @return Array of event id 如果没有命中事件则返回空值或者长度为零的数组
    */
  def checkDataPoint(componentId: Long, metricName: String, metricValue: Double): Array[Long]


  def checkDataPoint(point: DataPoint): Array[Long]


  def checkDataPointToThingBullet(point: DataPoint): Array[ThingBullet]
}
