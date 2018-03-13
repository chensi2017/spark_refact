package com.iiot.stream.spark

import com.iiot.stream.base.HTBaseContext
import com.iiot.stream.tools._
import org.apache.log4j.Logger
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

/**
  * Created by yuxiao on 2017/7/5.
  */
class HTStateContext extends HTBaseContext {
  @transient lazy val logger: Logger = Logger.getLogger(HTStateContext.getClass)

  /**
    * 统计一些指标
    */
  override def setExecutor(stream: DStream[(String, String)]): Unit = {
    val stateStatistics = new HTStateStatistics
    val jsonData = HTInputDStreamFormat.inputDStreamFormat(stream) //.persist(StorageLevel.MEMORY_AND_DISK_2)
    println("-----------------------------------------in setExecutor")

      // 1 统计所有datapoint数量
      stateStatistics.totalDP(jsonData)
      // 2 按租户统计所有的DataPoint数量
      stateStatistics.totalTenanidDP(jsonData)
      // 3 按租户与天数统计所有的DataPoint数量
      stateStatistics.totalTenantidDateDP(jsonData)
      // 4 按日期统计所有的DataPoint数量
      stateStatistics.totalDateDP(jsonData)
      // 5 按组件数统计所有的DataPoint数量
      stateStatistics.totalDPCompid(jsonData)
      // 6 所有的独立dn数
      stateStatistics.uniqueDN(jsonData)
      // 7 所有独立的设备数
      stateStatistics.uniqueThingid(jsonData)
      // 8 按天统计去重设备数
      stateStatistics.uniqueThingidDate(jsonData)
      // 9 按租户与天数统计所有去重的设备数
      stateStatistics.uniqueThingidTenantidDate(jsonData)
      // 10 按天与租户ID统计测点数
      stateStatistics.dateTidCompid(jsonData)
      // 1 30秒内独立dn数
      stateStatistics.uniqueDnActive(jsonData)
      // 12 根据租户ID统计30秒内独立thingid数
      stateStatistics.uniqueThingidTenantidActive(jsonData)
      // 13 根据租户ID统计30秒内独立compid数
      stateStatistics.dateTidCompidActive(jsonData)


  }
}

object HTStateContext {
  def main(args: Array[String]): Unit = {
    val configs = new MemConfig("spark_streamming.properties")
    val context = new HTStateContext

    if(configs.get("spark.run.type").equals("cluster")) {
      context.run(configs.get("spark.master"), configs.get("spark.appHStateName"))
    }else{
      context.run()
    }
  }
}